"""Fast, no-network unit tests for Sentinel's core primitives.

These reuse the existing functions directly (no pipeline, no LLM, no DB) so the
suite stays quick and deterministic.  Run with:  pytest tests/test_units.py
"""

import sys
sys.path.insert(0, ".")

from schemas import ColumnSchema, Criticality, RunMetrics
from observability.detection.statistical import compute_zscore, compute_psi, _DEGENERATE_Z
from observability.detection.rules import _severity_from_deviation
from schemas import SeverityLevel, ActionType, GateType
from governance.policy import evaluate_gate
from action.registry import get_action, is_blocked


# ── schema hash (schemas.py) ───────────────────────────────────────────────

def test_schema_hash_is_order_independent_and_stable():
    a = [ColumnSchema(name="amount", dtype="float64"),
         ColumnSchema(name="type", dtype="object")]
    b = [ColumnSchema(name="type", dtype="object"),
         ColumnSchema(name="amount", dtype="float64")]
    # Same columns in different order → identical hash (sorted by name internally).
    assert RunMetrics.compute_schema_hash(a) == RunMetrics.compute_schema_hash(b)
    # A dtype change → different hash.
    c = [ColumnSchema(name="amount", dtype="int64"),
         ColumnSchema(name="type", dtype="object")]
    assert RunMetrics.compute_schema_hash(a) != RunMetrics.compute_schema_hash(c)


# ── z-score (statistical.py) ───────────────────────────────────────────────

def test_zscore_normal_case():
    z = compute_zscore(10.0, [0.0, 1.0, 2.0, 1.0, 0.0])
    assert z > 3.0  # 10 is far above a low-mean, low-variance history


def test_zscore_zero_variance_degenerate():
    # Constant history but a different current value → maximal (finite) deviation.
    z = compute_zscore(2000.0, [5000.0] * 6)
    assert abs(z) == _DEGENERATE_Z
    assert z < 0  # current is below the constant baseline


def test_zscore_zero_variance_no_change():
    assert compute_zscore(5000.0, [5000.0] * 6) == 0.0


def test_zscore_too_short_history():
    assert compute_zscore(99.0, [1.0]) == 0.0


# ── PSI (statistical.py) ───────────────────────────────────────────────────

def test_psi_detects_categorical_shift():
    baseline = {"PAYMENT": 0.5, "CASH_OUT": 0.5}
    shifted = {"PAYMENT": 0.95, "CASH_OUT": 0.05}
    assert compute_psi(shifted, baseline) >= 0.2


def test_psi_identical_is_near_zero():
    d = {"PAYMENT": 0.5, "CASH_OUT": 0.5}
    assert compute_psi(d, d) < 0.01


# ── severity mapping (rules.py) ────────────────────────────────────────────

def test_severity_scales_with_criticality():
    # deviation 4 on a low-criticality dataset (mult 0.5) → score 2.0 → medium
    assert _severity_from_deviation(4.0, Criticality.low) == SeverityLevel.medium
    # same deviation on a critical dataset (mult 2.0) → score 8.0 → critical
    assert _severity_from_deviation(4.0, Criticality.critical) == SeverityLevel.critical


# ── gate policy (policy.py + registry.py) ──────────────────────────────────

def test_gate_safe_action_high_dataset_is_one_click():
    assert evaluate_gate(get_action(ActionType.rerun_job), Criticality.high) == GateType.one_click


def test_gate_safe_action_critical_dataset_escalates():
    assert evaluate_gate(get_action(ActionType.rerun_job), Criticality.critical) == GateType.typed_confirmation


def test_gate_medium_action_high_dataset_is_typed_confirmation():
    assert evaluate_gate(get_action(ActionType.backfill), Criticality.high) == GateType.typed_confirmation


def test_destructive_actions_are_not_registered():
    # patch_data / alter_schema are not even ActionType members → can't enter the system.
    assert not hasattr(ActionType, "patch_data")
    assert not hasattr(ActionType, "alter_schema")
    # none / manual are no-ops, not blocked; unknown registered actions only via registry.
    assert is_blocked(ActionType.none) is False


# ── extended coverage: validity / uniqueness / operational ─────────────────

from datetime import datetime, timezone
from schemas import (
    IntentConfig, ColumnRange, NumericStats, RunMetrics, ColumnSchema,
    OperationalSignals, JobStatus,
)
from observability.detection.rules import check_validity, check_uniqueness
from observability.detection.operational import check_operational


def _metrics(numeric_stats=None, duplicate_rate=0.0):
    now = datetime.now(timezone.utc)
    return RunMetrics(
        run_id="r1", dataset="transactions", stage="raw_transactions", ts_run=now,
        event_time_max=now, row_count=100, freshness_minutes=1.0, schema_hash="abc",
        schema=[ColumnSchema(name="amount", dtype="float64")],
        numeric_stats=numeric_stats or {}, duplicate_rate=duplicate_rate,
    )


def _intent(**kw):
    base = dict(dataset="transactions", owner="x", criticality="high")
    base.update(kw)
    return IntentConfig(**base)


def _ns(mn, mx):
    return NumericStats(mean=0, std=1, p05=0, p50=0, p95=0, min=mn, max=mx)


def test_validity_fires_on_out_of_range():
    intent = _intent(expected_ranges={"amount": ColumnRange(min=0, max=1e9)})
    # min below 0 → fires
    anoms = check_validity(_metrics({"amount": _ns(-5.0, 100.0)}), intent)
    assert len(anoms) == 1 and anoms[0].check_type.value == "validity"
    # within range → no fire
    assert check_validity(_metrics({"amount": _ns(10.0, 100.0)}), intent) == []


def test_validity_silent_when_unconfigured():
    assert check_validity(_metrics({"amount": _ns(-5.0, 100.0)}), _intent()) == []


def test_uniqueness_fires_above_threshold():
    intent = _intent(unique_key=["nameOrig", "amount"])
    assert check_uniqueness(_metrics(duplicate_rate=0.3), intent).check_type.value == "uniqueness"
    assert check_uniqueness(_metrics(duplicate_rate=0.0), intent) is None
    # no key configured → never fires
    assert check_uniqueness(_metrics(duplicate_rate=0.9), _intent()) is None


def _op(status, exit_code=0, duration=3.0, retries=0):
    now = datetime.now(timezone.utc)
    return OperationalSignals(run_id="r1", job_name="enriched", status=JobStatus(status),
                              started_at=now, ended_at=now, duration_seconds=duration,
                              retries=retries, exit_code=exit_code)


def test_operational_classifies_oom_and_timeout():
    intent = _intent(max_duration_seconds=30, max_retries=2)
    hist = [_op("success")] * 6
    oom = check_operational(_op("failed", exit_code=137), hist, "transactions", intent)
    assert any(a.metric == "operational.oom" for a in oom)
    to = check_operational(_op("failed", exit_code=124, duration=180), hist, "transactions", intent)
    assert any(a.metric == "operational.timeout" for a in to)


def test_operational_retry_storm_and_clean():
    intent = _intent(max_duration_seconds=30, max_retries=2)
    hist = [_op("success")] * 6
    rs = check_operational(_op("success", retries=5), hist, "transactions", intent)
    assert any(a.metric == "operational.retry_storm" for a in rs)
    # healthy job → no operational anomalies
    assert check_operational(_op("success"), hist, "transactions", intent) == []


# ── evaluation: earned true positives (matched check-type) ─────────────────

from evaluation.detection_metrics import DetectionResult, evaluate_detection


def test_detection_tp_requires_matching_check():
    # row_drop expects a 'volume' anomaly. A 'schema' anomaly must NOT earn a TP.
    right = DetectionResult("r1", "row_drop", "", ["row_count"], ["volume"], True)
    wrong = DetectionResult("r2", "row_drop", "", ["schema_hash"], ["schema"], True)
    assert right.matched is True
    assert wrong.matched is False


def test_evaluate_detection_units_are_per_run():
    results = [
        DetectionResult("r1", "row_drop", "", ["row_count"], ["volume"], True),          # TP
        DetectionResult("r2", "column_null", "amount", ["schema_hash"], ["schema"], True),  # wrong check → FN
    ]
    # A clean run with 3 spurious anomalies is ONE false positive (per-run), not 3.
    m = evaluate_detection(results, clean_runs_detected=[["a", "b", "c"], []])
    assert m.true_positives == 1 and m.false_negatives == 1
    assert m.false_positives == 1 and m.true_negatives == 1


# ── LLM contract failure path (no API tokens: client is mocked) ────────────

from types import SimpleNamespace
from reasoning.reporter import Reporter
from schemas import Anomaly, CheckType as _CT, SeverityLevel as _SV, ReasoningContext


class _FakeMessages:
    def create(self, **_kw):
        # The model rambles instead of returning the strict JSON object.
        return SimpleNamespace(content=[SimpleNamespace(text="Sure! Here is my analysis...")])


class _FakeClient:
    def __init__(self):
        self.messages = _FakeMessages()


def test_reporter_returns_report_invalid_on_malformed_json():
    r = Reporter.__new__(Reporter)          # bypass __init__ (no key / network)
    r._client, r._model = _FakeClient(), "test-model"
    ctx = ReasoningContext(
        anomaly=Anomaly(anomaly_id="a1", run_id="r1", dataset="transactions",
                        stage="enriched", metric="row_count", check_type=_CT.volume,
                        observed=1500, expected=10000, deviation=-20.0,
                        severity_hint=_SV.critical,
                        detected_at=datetime.now(timezone.utc)),
        intent=_intent(),
    )
    report, valid = r.generate_report(ctx)
    # Non-JSON after the single retry → (None, False) → orchestrator marks report_invalid.
    assert report is None and valid is False
