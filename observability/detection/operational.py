"""
Sentinel — Operational anomaly detection (spec extension).

Turns the *operational* signal stream (job status, duration, retries, exit code)
into first-class anomalies, so compute/infrastructure failures are detected the
same way a production orchestrator (Prefect/Airflow/Spark) exposes them:

* **OOM**          — job failed with exit code 137 (OOM-killer).
* **timeout**      — job failed with exit code 124, or duration over the SLA.
* **job_failed**   — any other failed/skipped job.
* **slow**         — duration far above the rolling baseline (compute pressure).
* **retry_storm**  — retries above the configured ceiling (e.g. API 429s).

Each anomaly carries ``check_type=operational`` and a reason encoded in the
metric (``operational.<reason>``).  Thresholds are conservative and SLA-driven,
so healthy runs never fire.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from config import config
from schemas import (
    Anomaly,
    CheckType,
    Criticality,
    IntentConfig,
    OperationalSignals,
    SeverityLevel,
)
from observability.detection.statistical import compute_zscore

logger = logging.getLogger(__name__)

# Exit-code → reason classification for failed jobs.
_OOM_EXIT = 137       # 128 + SIGKILL (OOM-killer)
_TIMEOUT_EXIT = 124   # GNU coreutils `timeout` convention


def _anom(op: OperationalSignals, dataset: str, reason: str, observed, expected,
          deviation: float, severity: SeverityLevel) -> Anomaly:
    return Anomaly(
        anomaly_id=uuid.uuid4().hex[:16],
        run_id=op.run_id,
        dataset=dataset,
        stage=op.job_name,                 # job_name == pipeline stage
        metric=f"operational.{reason}",
        check_type=CheckType.operational,
        observed=observed,
        expected=expected,
        deviation=deviation,
        severity_hint=severity,
        detected_at=datetime.now(tz=timezone.utc),
    )


def check_operational(
    op: OperationalSignals,
    history: List[OperationalSignals],
    dataset: str,
    intent: IntentConfig,
) -> List[Anomaly]:
    """Detect operational anomalies for one job's signals.

    Parameters
    ----------
    op:
        The current run's :class:`OperationalSignals` for this stage/job.
    history:
        Prior signals for the same job (rolling baseline for duration).
    dataset:
        Dataset name (the signal doesn't carry it).
    intent:
        Dataset config — ``max_duration_seconds`` / ``max_retries`` SLAs.
    """
    anomalies: List[Anomaly] = []
    crit = intent.criticality

    # ── Failure (with exit-code classification) ────────────────────────
    if op.status.value in {"failed", "skipped"}:
        if op.exit_code == _OOM_EXIT:
            reason = "oom"
        elif op.exit_code == _TIMEOUT_EXIT:
            reason = "timeout"
        else:
            reason = "job_failed"
        anomalies.append(_anom(
            op, dataset, reason,
            observed={"status": op.status.value, "exit_code": op.exit_code},
            expected="success", deviation=1.0,
            severity=SeverityLevel.critical if crit in (Criticality.high, Criticality.critical)
            else SeverityLevel.high,
        ))

    # ── Duration: SLA breach and/or statistical spike ──────────────────
    if op.duration_seconds is not None:
        sla = intent.max_duration_seconds
        if sla is not None and op.duration_seconds > sla:
            ratio = op.duration_seconds / max(sla, 1e-9)
            anomalies.append(_anom(
                op, dataset, "timeout",
                observed=round(op.duration_seconds, 2), expected=f"<= {sla}s",
                deviation=round(ratio, 3),
                severity=_dur_sev(ratio, crit),
            ))
        else:
            hist = [h.duration_seconds for h in history
                    if h.duration_seconds is not None]
            if len(hist) >= config.MIN_BASELINE:
                z = compute_zscore(op.duration_seconds, hist)
                if z >= 3.0:   # only *slower* than baseline is interesting
                    anomalies.append(_anom(
                        op, dataset, "slow",
                        observed=round(op.duration_seconds, 2),
                        expected="~baseline", deviation=round(z, 3),
                        severity=SeverityLevel.medium if z < 5 else SeverityLevel.high,
                    ))

    # ── Retry storm ────────────────────────────────────────────────────
    if intent.max_retries is not None and op.retries > intent.max_retries:
        anomalies.append(_anom(
            op, dataset, "retry_storm",
            observed=op.retries, expected=f"<= {intent.max_retries}",
            deviation=float(op.retries),
            severity=SeverityLevel.medium if op.retries <= intent.max_retries + 2
            else SeverityLevel.high,
        ))

    if anomalies:
        logger.info("Operational anomalies on %s/%s: %s", dataset, op.job_name,
                    [a.metric for a in anomalies])
    return anomalies


def _dur_sev(ratio: float, crit: Criticality) -> SeverityLevel:
    base = SeverityLevel.high if ratio >= 2 else SeverityLevel.medium
    if crit == Criticality.critical and base == SeverityLevel.high:
        return SeverityLevel.critical
    return base
