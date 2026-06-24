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
