"""
Sentinel — Control-Loop Orchestrator (Spec §10).

Implements one full cycle of the operating loop:

    Observe → (detect) → Reason → Propose → (gate) → Remember

for a completed pipeline run.  This is the missing glue that turns the
individual layers into a working system: it runs detection **before** the
current run's metrics are persisted (so the run is never part of its own
baseline), creates and persists :class:`~schemas.Incident` records, writes
the audit trail, evaluates the governance gate, routes high/critical
incidents to Slack, and finally runs the auto-resolution detector.

Public entry point: :func:`process_run`.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import config
from schemas import (
    ActionType,
    Anomaly,
    AuditEvent,
    Incident,
    IncidentStatus,
    IntentConfig,
    ReasoningOutput,
    SeverityLevel,
)
from observability.metrics import compute_metrics
from observability.detection.engine import run_detection, group_related
from memory.retrieve import retrieve_similar
from reasoning.context import assemble_context
from reasoning.reporter import Reporter
from action.registry import get_action, is_blocked
from governance.policy import evaluate_gate
from governance.audit import log_event
from governance.resolution import check_auto_resolution
from routing.slack import send_incident_to_slack

logger = logging.getLogger(__name__)

_SEVERITY_RANK = {
    SeverityLevel.low: 0,
    SeverityLevel.medium: 1,
    SeverityLevel.high: 2,
    SeverityLevel.critical: 3,
}

# Action types that never require an approval gate (no real action proposed).
_NON_ACTIONABLE = {ActionType.none, ActionType.manual}


def _max_severity(anomalies: List[Anomaly]) -> SeverityLevel:
    """Return the highest ``severity_hint`` across *anomalies* (rules-only)."""
    return max(
        (a.severity_hint for a in anomalies),
        key=lambda s: _SEVERITY_RANK[s],
        default=SeverityLevel.low,
    )


def process_run(
    manifest: Dict[str, Any],
    store,
    intent: IntentConfig,
    memory_store=None,
    reporter: Optional[Reporter] = None,
    slack_webhook: str = "",
    auto_resolve: bool = True,
) -> List[Incident]:
    """Process one completed pipeline run end-to-end (spec §10).

    Parameters
    ----------
    manifest:
        The dict returned by :func:`pipeline.flows.run_pipeline`.
    store:
        :class:`~observability.store.SentinelStore`.
    intent:
        The dataset's :class:`~schemas.IntentConfig`.
    memory_store:
        Optional :class:`~memory.store.MemoryStore` for retrieval/writing.
    reporter:
        Optional :class:`~reasoning.reporter.Reporter`.  When ``None`` the
        LLM is skipped and incidents carry a rules-only severity with no
        report (useful for fast, offline evaluation).
    slack_webhook:
        Slack incoming-webhook URL; high/critical incidents are routed here.
    auto_resolve:
        When ``True`` (default) the auto-resolution detector runs after the
        cycle.

    Returns
    -------
    list[Incident]
        The incidents created during this cycle.
    """
    run_id: str = manifest["run_id"]
    dataset = intent.dataset
    reference_now: datetime = manifest.get("reference_now") or datetime.now(timezone.utc)

    # ── 1. Observe + detect (per stage), then persist ───────────────────────
    all_anomalies: List[Anomaly] = []
    for stage_name, batch in manifest["batches"].items():
        metrics = compute_metrics(
            run_id=run_id,
            dataset=dataset,
            stage=stage_name,
            batch=batch,
            key_columns=intent.key_columns,
            reference_now=reference_now,
            unique_key=intent.unique_key,
        )

        # Detect BEFORE persisting so the current run is not part of its own
        # rolling baseline (and schema diff compares against the true prev run).
        # The stage's operational signal feeds OOM/timeout/slow/retry detection.
        op_sig = manifest.get("operational_signals", {}).get(stage_name)
        anomalies = run_detection(metrics, store, intent, op=op_sig)

        # Persist metrics + raw batch rows (the latter enables quarantine).
        store.save_metrics(metrics)
        try:
            store.save_batch(stage_name, run_id, batch)
        except Exception as exc:
            # Best-effort: a schema-change fault legitimately can't append to a
            # fixed-schema warehouse table; quarantine simply won't apply there.
            logger.warning("Skipped batch persistence for stage %s: %s", stage_name, exc)

        for a in anomalies:
            log_event(
                store,
                event=AuditEvent.anomaly_detected,
                detail={"metric": a.metric, "stage": stage_name, "severity": a.severity_hint.value},
            )
        all_anomalies.extend(anomalies)

    # ── 2. Persist operational signals ──────────────────────────────────────
    for op_sig in manifest.get("operational_signals", {}).values():
        store.save_ops_signals(op_sig)
    if manifest.get("fault_operational_signal"):
        store.save_ops_signals(manifest["fault_operational_signal"])

    # ── 3. Reason + create incidents (per anomaly group) ────────────────────
    incidents: List[Incident] = []
    for group in group_related(all_anomalies):
        incident = _build_incident(
            group, dataset, run_id, reference_now, store, intent, memory_store, reporter
        )
        incidents.append(incident)

        # Route high/critical to Slack (no-op if webhook unset / low severity).
        if slack_webhook:
            try:
                send_incident_to_slack(incident, slack_webhook)
            except Exception:
                logger.exception("Slack routing failed for incident %s", incident.incident_id)

    # ── 4. Auto-resolution sweep ────────────────────────────────────────────
    if auto_resolve and memory_store is not None:
        try:
            resolved = check_auto_resolution(store, memory_store)
            if resolved:
                logger.info("Auto-resolved %d incident(s): %s", len(resolved), resolved)
        except Exception:
            logger.exception("Auto-resolution sweep failed")

    return incidents


def _build_incident(
    group: List[Anomaly],
    dataset: str,
    run_id: str,
    reference_now: datetime,
    store,
    intent: IntentConfig,
    memory_store,
    reporter: Optional[Reporter],
) -> Incident:
    """Assemble context, run reasoning, and persist one incident (spec §10)."""
    primary = group[0]
    stage = primary.stage

    # Retrieve similar past incidents (memory-augmented reasoning).
    similar = []
    if memory_store is not None:
        try:
            similar = retrieve_similar(memory_store, primary, top_k=config.MEMORY_TOP_K)
        except Exception:
            logger.exception("Memory retrieval failed for anomaly %s", primary.anomaly_id)

    recent = store.get_recent_metrics(dataset, stage, n=config.BASELINE_WINDOW)
    ops = store.get_ops_signals(run_id)
    schema_current = recent[0].schema_ if recent else []

    ctx = assemble_context(
        anomalies=group,
        intent=intent,
        recent_metrics=recent,
        operational=ops,
        schema_current=schema_current,
        similar_incidents=similar,
    )

    report: Optional[ReasoningOutput] = None
    status = IncidentStatus.open

    if reporter is not None:
        report, valid = reporter.generate_report(ctx)
        if not valid:
            # Spec §7.6/§9: invalid LLM output → report_invalid + rules-only severity.
            status = IncidentStatus.report_invalid
            logger.warning("Incident for %s/%s marked report_invalid", dataset, stage)

    incident = Incident(
        incident_id=uuid.uuid4().hex[:12],
        created_at=reference_now,
        dataset=dataset,
        stage=stage,
        run_id=run_id,
        anomalies=group,
        context_used=ctx,
        report=report,
        status=status,
    )

    # ── Gate evaluation ─────────────────────────────────────────────────────
    if report is not None and status != IncidentStatus.report_invalid:
        action = report.suggested_action
        if action.type in _NON_ACTIONABLE or is_blocked(action.type):
            incident.status = IncidentStatus.open  # report-only, no action proposed
        else:
            action_def = get_action(action.type)
            if action_def is not None:
                gate = evaluate_gate(action_def, intent.criticality)
                log_event(
                    store,
                    event=AuditEvent.gate_evaluated,
                    detail={"action": action.type.value, "gate": gate.value},
                )
                incident.status = IncidentStatus.awaiting_approval

    store.save_incident(incident)
    log_event(
        store,
        event=AuditEvent.incident_created,
        incident_id=incident.incident_id,
        detail={
            "anomaly_count": len(group),
            "severity": (report.severity.value if report else _max_severity(group).value),
            "status": incident.status.value,
        },
    )
    if report is not None:
        log_event(
            store,
            event=AuditEvent.report_generated,
            incident_id=incident.incident_id,
            detail={"caused_by": report.caused_by.value, "confidence": report.confidence},
        )

    return incident
