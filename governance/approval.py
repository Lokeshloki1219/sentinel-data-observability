"""
Sentinel — Governance: Reason-coded Approval Routing (§7.9).

Implements all 4 resolution reason routes:
  - not_a_problem → SuppressionRule + mark suppressed
  - will_fix_manually → store note to Memory + mark acknowledged_manual
  - wrong_diagnosis → keep open + negative retrieval signal to Memory
  - defer → mark snoozed
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from schemas import (
    AuditEvent,
    ActorType,
    DecisionType,
    Incident,
    IncidentStatus,
    MemoryRecord,
    ReasonCode,
    Resolution,
)
from governance.audit import log_event
from governance.suppression import create_suppression_from_incident
from memory.embed import build_summary_text

logger = logging.getLogger(__name__)


def process_resolution(
    resolution: Resolution,
    incident: Incident,
    store,
    memory_store,
) -> Incident:
    """Process a human resolution decision and route by reason code.

    Implements the full 4-way routing described in spec §7.9.

    Args:
        resolution: The human decision with reason code.
        incident: The incident being resolved.
        store: SentinelStore for persistence.
        memory_store: MemoryStore for vector DB operations.

    Returns:
        The updated Incident with new status.
    """
    # 1. Persist the resolution itself
    incident.resolution = resolution
    store.save_resolution(resolution)

    # 2. Audit the resolution
    log_event(
        store,
        event=AuditEvent.resolution_recorded,
        incident_id=incident.incident_id,
        detail={
            "decision": resolution.decision.value,
            "reason": resolution.reason.value,
            "decided_by": resolution.decided_by,
        },
        actor=ActorType.human,
    )

    # 3. Route by decision type
    if resolution.decision in (DecisionType.approved, DecisionType.modified):
        incident.status = IncidentStatus.awaiting_approval
        logger.info(
            "Incident %s: approved (decision=%s) — ready for executor.",
            incident.incident_id,
            resolution.decision.value,
        )
    elif resolution.decision == DecisionType.rejected:
        _route_rejection(resolution, incident, store, memory_store)
    elif resolution.decision == DecisionType.snoozed:
        incident.status = IncidentStatus.snoozed
        logger.info("Incident %s: snoozed (defer).", incident.incident_id)

    # 4. Persist updated incident
    store.update_incident(incident)
    return incident


def _route_rejection(
    resolution: Resolution,
    incident: Incident,
    store,
    memory_store,
) -> None:
    """Route a rejection by its reason code (§7.9)."""

    reason = resolution.reason

    # ── not_a_problem → suppression ────────────────────────────────
    if reason == ReasonCode.not_a_problem:
        rule = create_suppression_from_incident(incident, store)
        incident.status = IncidentStatus.suppressed
        logger.info(
            "Incident %s: not_a_problem — created SuppressionRule %s, marked suppressed.",
            incident.incident_id,
            rule.rule_id,
        )

    # ── will_fix_manually → memory note ────────────────────────────
    elif reason == ReasonCode.will_fix_manually:
        incident.status = IncidentStatus.acknowledged_manual
        # Build summary including the manual fix note and store to memory
        summary = build_summary_text(incident)
        if resolution.manual_fix_note:
            summary += f"\n\nManual fix note: {resolution.manual_fix_note}"

        record = MemoryRecord(
            incident_id=incident.incident_id,
            dataset=incident.dataset,
            check_type=incident.anomalies[0].check_type.value if incident.anomalies else "unknown",
            summary_text=summary,
            report=incident.report,
            outcome=incident.outcome,
        )
        memory_store.add_record(record)
        logger.info(
            "Incident %s: will_fix_manually — stored note to memory, marked acknowledged_manual.",
            incident.incident_id,
        )

    # ── wrong_diagnosis → negative signal ──────────────────────────
    elif reason == ReasonCode.wrong_diagnosis:
        incident.status = IncidentStatus.open  # keep open
        # Record a NEGATIVE retrieval signal so the LLM learns what NOT to diagnose
        summary = build_summary_text(incident)
        summary += "\n\n[NEGATIVE SIGNAL] This diagnosis was marked as WRONG by a human reviewer."

        record = MemoryRecord(
            incident_id=incident.incident_id,
            dataset=incident.dataset,
            check_type=incident.anomalies[0].check_type.value if incident.anomalies else "unknown",
            summary_text=summary,
            report=incident.report,
            outcome=incident.outcome,
        )
        memory_store.add_record(record, is_negative=True)
        logger.info(
            "Incident %s: wrong_diagnosis — negative signal written to memory, kept open.",
            incident.incident_id,
        )

    # ── defer → snoozed ───────────────────────────────────────────
    elif reason == ReasonCode.defer:
        incident.status = IncidentStatus.snoozed
        logger.info("Incident %s: defer — marked snoozed.", incident.incident_id)

    else:
        # Default: just mark rejected
        incident.status = IncidentStatus.rejected
        logger.warning(
            "Incident %s: unrecognized reason '%s' — marked rejected.",
            incident.incident_id,
            reason.value,
        )
