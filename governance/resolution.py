"""
Sentinel — Governance: Auto-Resolution Detector (§12).

Watches open/acted incidents on each subsequent run.
If the offending metric returns to baseline within K runs,
sets Outcome and writes a MemoryRecord.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from config import config
from schemas import (
    AuditEvent,
    ActorType,
    Incident,
    IncidentStatus,
    MemoryRecord,
    Outcome,
    ResolutionMethod,
    RunMetrics,
)
from governance.audit import log_event
from memory.embed import build_summary_text
from observability.detection.statistical import compute_zscore

logger = logging.getLogger(__name__)


def _as_aware(dt: datetime) -> datetime:
    """Coerce a possibly-naive datetime to timezone-aware UTC."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


# Statuses eligible for auto-resolution checking
_WATCHABLE = {
    IncidentStatus.open,
    IncidentStatus.acted,
    IncidentStatus.awaiting_approval,
}


def check_auto_resolution(
    store,
    memory_store,
    k: int = None,
) -> List[str]:
    """Scan all watchable incidents for auto-resolution.

    For each open/acted incident, check the last K runs for the same
    dataset/stage. If the offending metric has returned to baseline
    (|z-score| < 2), mark the incident as resolved.

    Args:
        store: SentinelStore instance.
        memory_store: MemoryStore instance (for writing MemoryRecords).
        k: Number of consecutive baseline runs required (default from config).

    Returns:
        List of incident IDs that were auto-resolved.
    """
    if k is None:
        k = config.AUTO_RESOLVE_K

    resolved_ids: List[str] = []
    open_incidents = store.get_open_incidents()

    for incident in open_incidents:
        if incident.status not in _WATCHABLE:
            continue

        if not incident.anomalies:
            continue

        primary = incident.anomalies[0]
        metric_name = primary.metric
        dataset = incident.dataset
        stage = incident.stage

        # Get recent metrics since the incident was created
        recent = store.get_recent_metrics(dataset, stage, n=k + 5)
        # Filter to runs AFTER the incident was created (tz-safe comparison)
        created_at = _as_aware(incident.created_at)
        post_incident = [
            m for m in recent
            if _as_aware(m.ts_run) > created_at
        ]

        if len(post_incident) < k:
            # Not enough subsequent runs yet
            continue

        # Check if the metric has returned to baseline in the last K runs
        if _metric_in_baseline(metric_name, post_incident[-k:], recent):
            # Auto-resolve!
            now = datetime.now(timezone.utc)
            resolution_method = (
                ResolutionMethod.action
                if incident.status == IncidentStatus.acted
                else ResolutionMethod.auto
            )

            time_delta = (now - _as_aware(incident.created_at)).total_seconds() / 60.0

            outcome = Outcome(
                incident_id=incident.incident_id,
                resolved=True,
                resolved_at=now,
                time_to_resolution_minutes=round(time_delta, 2),
                resolution_method=resolution_method,
                fix_worked=True,
            )

            incident.status = IncidentStatus.resolved
            incident.outcome = outcome

            store.save_outcome(outcome)
            store.update_incident(incident)

            # Write to memory
            summary = build_summary_text(incident)
            record = MemoryRecord(
                incident_id=incident.incident_id,
                dataset=incident.dataset,
                check_type=primary.check_type.value,
                summary_text=summary,
                report=incident.report,
                outcome=outcome,
            )
            memory_store.add_record(record)

            # Audit
            log_event(
                store,
                event=AuditEvent.outcome_recorded,
                incident_id=incident.incident_id,
                detail={
                    "resolved": True,
                    "resolution_method": resolution_method.value,
                    "fix_worked": True,
                    "time_to_resolution_minutes": outcome.time_to_resolution_minutes,
                },
            )

            resolved_ids.append(incident.incident_id)
            logger.info(
                "Auto-resolved incident %s (%s/%s/%s) after %d baseline runs. "
                "TTR=%.1f min, method=%s.",
                incident.incident_id,
                dataset,
                stage,
                metric_name,
                k,
                time_delta,
                resolution_method.value,
            )

    return resolved_ids


def _metric_in_baseline(
    metric_name: str,
    recent_k: List[RunMetrics],
    full_history: List[RunMetrics],
) -> bool:
    """Check if a metric has returned to baseline across K recent runs.

    Uses the full history to compute the baseline mean/std, then checks
    if all K recent values are within |z| < 2.
    """
    values = _extract_metric_values(metric_name, full_history)
    recent_values = _extract_metric_values(metric_name, recent_k)

    if not values or not recent_values:
        return False

    # All recent values must be within baseline
    for val in recent_values:
        z = compute_zscore(val, values)
        if abs(z) >= 2.0:
            return False

    return True


def _extract_metric_values(
    metric_name: str,
    metrics_list: List[RunMetrics],
) -> List[float]:
    """Extract numeric values for a named metric from a list of RunMetrics.

    Supports dotted metric names like 'null_rate.amount', 'volume.row_count',
    'numeric_stats.amount.mean', etc.
    """
    values: List[float] = []
    parts = metric_name.split(".")

    for m in metrics_list:
        try:
            if parts[0] == "volume" or parts[0] == "row_count":
                values.append(float(m.row_count))
            elif parts[0] == "freshness" or parts[0] == "freshness_minutes":
                values.append(float(m.freshness_minutes))
            elif parts[0] == "null_rate" and len(parts) > 1:
                col = parts[1]
                if col in m.null_rate:
                    values.append(m.null_rate[col])
            elif parts[0] == "numeric_stats" and len(parts) > 2:
                col, stat = parts[1], parts[2]
                if col in m.numeric_stats:
                    values.append(getattr(m.numeric_stats[col], stat, 0.0))
            # For schema checks, not a numeric value — skip
        except (AttributeError, KeyError, IndexError, TypeError):
            continue

    return values
