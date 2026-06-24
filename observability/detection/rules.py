"""
Sentinel — Rule-Based Anomaly Detection.

Each ``check_*`` function evaluates one category of data-quality invariant
and returns either a single :class:`~schemas.Anomaly` (or ``None``) or a
list of anomalies.  The detection engine (:mod:`engine`) calls every check
on each run and aggregates the results.

Severity hints are derived from ``deviation × criticality`` so that low-
criticality datasets can drift further before triggering high-severity
alerts.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np

from schemas import (
    Anomaly,
    CheckType,
    Criticality,
    IntentConfig,
    RunMetrics,
    SeverityLevel,
)
from observability.detection.statistical import (
    compute_ks_test,
    compute_psi,
    compute_zscore,
)

logger = logging.getLogger(__name__)

# ── Severity mapping helpers ──────────────────────────────────────────────

# Criticality multiplier: higher criticality → lower deviation threshold
# before escalating severity.
_CRIT_MULT: Dict[Criticality, float] = {
    Criticality.low: 0.5,
    Criticality.medium: 1.0,
    Criticality.high: 1.5,
    Criticality.critical: 2.0,
}


def _severity_from_deviation(
    deviation: float,
    criticality: Criticality,
) -> SeverityLevel:
    """Map *deviation* × *criticality* to a severity level.

    The effective score is ``abs(deviation) × criticality_multiplier``.

    ==============  ===================
    Score range     Severity
    ==============  ===================
    < 1.5           low
    1.5 – 3.0       medium
    3.0 – 5.0       high
    ≥ 5.0           critical
    ==============  ===================
    """
    score = abs(deviation) * _CRIT_MULT.get(criticality, 1.0)
    if score >= 5.0:
        return SeverityLevel.critical
    if score >= 3.0:
        return SeverityLevel.high
    if score >= 1.5:
        return SeverityLevel.medium
    return SeverityLevel.low


def _make_anomaly(
    run_id: str,
    dataset: str,
    stage: str,
    metric: str,
    check_type: CheckType,
    observed: object,
    expected: object,
    deviation: float,
    severity: SeverityLevel,
) -> Anomaly:
    """Helper to construct an Anomaly with a fresh UUID and timestamp."""
    return Anomaly(
        anomaly_id=uuid.uuid4().hex[:16],
        run_id=run_id,
        dataset=dataset,
        stage=stage,
        metric=metric,
        check_type=check_type,
        observed=observed,
        expected=expected,
        deviation=deviation,
        severity_hint=severity,
        detected_at=datetime.now(tz=timezone.utc),
    )


# ── Individual check functions ────────────────────────────────────────────


def check_freshness(
    metrics: RunMetrics,
    intent: IntentConfig,
) -> Optional[Anomaly]:
    """Check whether data freshness exceeds the SLA.

    Returns an :class:`Anomaly` if ``metrics.freshness_minutes`` exceeds
    ``intent.freshness_sla_minutes``, otherwise ``None``.
    """
    sla = intent.freshness_sla_minutes
    if sla <= 0:
        return None
    if metrics.freshness_minutes <= sla:
        return None

    deviation = metrics.freshness_minutes / sla  # ratio ≥ 1
    severity = _severity_from_deviation(deviation, intent.criticality)

    return _make_anomaly(
        run_id=metrics.run_id,
        dataset=metrics.dataset,
        stage=metrics.stage,
        metric="freshness_minutes",
        check_type=CheckType.freshness,
        observed=round(metrics.freshness_minutes, 2),
        expected=sla,
        deviation=round(deviation, 4),
        severity=severity,
    )


def check_volume(
    metrics: RunMetrics,
    intent: IntentConfig,
    history: List[RunMetrics],
) -> Optional[Anomaly]:
    """Detect abnormal row-count swings.

    Uses either the explicit ``intent.expected_volume`` bounds or a
    rolling z-score computed from *history*.
    """
    # Absolute bounds check
    ev = intent.expected_volume
    if ev is not None:
        if metrics.row_count < ev.min_rows:
            deviation = (ev.min_rows - metrics.row_count) / max(ev.min_rows, 1)
            severity = _severity_from_deviation(deviation * 5, intent.criticality)
            return _make_anomaly(
                run_id=metrics.run_id,
                dataset=metrics.dataset,
                stage=metrics.stage,
                metric="row_count",
                check_type=CheckType.volume,
                observed=metrics.row_count,
                expected={"min": ev.min_rows, "max": ev.max_rows},
                deviation=round(deviation, 4),
                severity=severity,
            )
        if metrics.row_count > ev.max_rows:
            deviation = (metrics.row_count - ev.max_rows) / max(ev.max_rows, 1)
            severity = _severity_from_deviation(deviation * 5, intent.criticality)
            return _make_anomaly(
                run_id=metrics.run_id,
                dataset=metrics.dataset,
                stage=metrics.stage,
                metric="row_count",
                check_type=CheckType.volume,
                observed=metrics.row_count,
                expected={"min": ev.min_rows, "max": ev.max_rows},
                deviation=round(deviation, 4),
                severity=severity,
            )

    # Statistical z-score check against history
    if len(history) < 2:
        return None

    hist_counts = [float(h.row_count) for h in history]
    z = compute_zscore(float(metrics.row_count), hist_counts)

    if abs(z) < 2.0:
        return None

    severity = _severity_from_deviation(z, intent.criticality)
    return _make_anomaly(
        run_id=metrics.run_id,
        dataset=metrics.dataset,
        stage=metrics.stage,
        metric="row_count",
        check_type=CheckType.volume,
        observed=metrics.row_count,
        expected=round(float(np.mean(hist_counts)), 2),
        deviation=round(z, 4),
        severity=severity,
    )


def check_null_rate(
    metrics: RunMetrics,
    intent: IntentConfig,
    history: List[RunMetrics],
) -> List[Anomaly]:
    """Detect null-rate spikes in key columns.

    For each column, first checks the explicit ``accepted_null_pct``
    threshold from intent; if none is set, falls back to a rolling
    z-score against *history*.
    """
    anomalies: List[Anomaly] = []

    for col, rate in metrics.null_rate.items():
        # Explicit threshold
        threshold = intent.accepted_null_pct.get(col)
        if threshold is not None:
            if rate > threshold:
                deviation = rate / max(threshold, 1e-9)
                severity = _severity_from_deviation(deviation, intent.criticality)
                anomalies.append(
                    _make_anomaly(
                        run_id=metrics.run_id,
                        dataset=metrics.dataset,
                        stage=metrics.stage,
                        metric=f"null_rate.{col}",
                        check_type=CheckType.null_rate,
                        observed=round(rate, 4),
                        expected=threshold,
                        deviation=round(deviation, 4),
                        severity=severity,
                    )
                )
            continue

        # Statistical fallback
        hist_rates = [
            h.null_rate.get(col, 0.0) for h in history if col in h.null_rate
        ]
        if len(hist_rates) < 2:
            continue
        z = compute_zscore(rate, hist_rates)
        if abs(z) < 2.0:
            continue
        severity = _severity_from_deviation(z, intent.criticality)
        anomalies.append(
            _make_anomaly(
                run_id=metrics.run_id,
                dataset=metrics.dataset,
                stage=metrics.stage,
                metric=f"null_rate.{col}",
                check_type=CheckType.null_rate,
                observed=round(rate, 4),
                expected=round(float(np.mean(hist_rates)), 4),
                deviation=round(z, 4),
                severity=severity,
            )
        )

    return anomalies


def check_schema(
    metrics: RunMetrics,
    prev_metrics: Optional[RunMetrics],
) -> Optional[Anomaly]:
    """Detect schema drift by comparing schema hashes.

    Returns an anomaly if the current schema hash differs from the
    previous run's hash.  Always ``high`` severity because silent schema
    changes can cascade unpredictably.
    """
    if prev_metrics is None:
        return None
    if metrics.schema_hash == prev_metrics.schema_hash:
        return None

    current_cols = {c.name for c in metrics.schema_}
    prev_cols = {c.name for c in prev_metrics.schema_}
    added = current_cols - prev_cols
    removed = prev_cols - current_cols

    return _make_anomaly(
        run_id=metrics.run_id,
        dataset=metrics.dataset,
        stage=metrics.stage,
        metric="schema_hash",
        check_type=CheckType.schema,
        observed=metrics.schema_hash,
        expected=prev_metrics.schema_hash,
        deviation=1.0,
        severity=SeverityLevel.high,
    )


def check_distribution(
    metrics: RunMetrics,
    history: List[RunMetrics],
    key_columns: List[str],
) -> List[Anomaly]:
    """Detect distribution shifts in numeric and categorical key columns.

    Numeric columns: uses K-S test against the most recent baseline
    ``numeric_stats`` values.
    Categorical columns: uses PSI against the averaged baseline
    distribution.
    """
    anomalies: List[Anomaly] = []

    if len(history) < 2:
        return anomalies

    # ── Numeric distribution shift via z-score on stats ────────────────
    for col in key_columns:
        cur_stats = metrics.numeric_stats.get(col)
        if cur_stats is None:
            continue

        hist_means = [
            h.numeric_stats[col].mean
            for h in history
            if col in h.numeric_stats
        ]
        if len(hist_means) < 2:
            continue

        z = compute_zscore(cur_stats.mean, hist_means)
        if abs(z) < 2.5:
            continue

        # Approximate severity from the deviation
        severity = _severity_from_deviation(z, Criticality.medium)
        anomalies.append(
            _make_anomaly(
                run_id=metrics.run_id,
                dataset=metrics.dataset,
                stage=metrics.stage,
                metric=f"distribution.{col}",
                check_type=CheckType.distribution,
                observed=round(cur_stats.mean, 4),
                expected=round(float(np.mean(hist_means)), 4),
                deviation=round(z, 4),
                severity=severity,
            )
        )

    # ── Categorical distribution shift via PSI ─────────────────────────
    for col in key_columns:
        cur_dist = metrics.categorical_dist.get(col)
        if cur_dist is None:
            continue

        # Build an averaged baseline from history
        baseline: Dict[str, float] = {}
        count = 0
        for h in history:
            hdist = h.categorical_dist.get(col)
            if hdist is None:
                continue
            count += 1
            for k, v in hdist.items():
                baseline[k] = baseline.get(k, 0.0) + v
        if count == 0:
            continue
        baseline = {k: v / count for k, v in baseline.items()}

        psi = compute_psi(cur_dist, baseline)
        if psi < 0.10:
            continue

        deviation = psi / 0.10  # normalise so 0.10 → 1.0
        severity = _severity_from_deviation(deviation, Criticality.medium)
        anomalies.append(
            _make_anomaly(
                run_id=metrics.run_id,
                dataset=metrics.dataset,
                stage=metrics.stage,
                metric=f"distribution.{col}",
                check_type=CheckType.distribution,
                observed=cur_dist,
                expected=baseline,
                deviation=round(psi, 4),
                severity=severity,
            )
        )

    return anomalies
