"""
Sentinel — Append-Only Audit Log (Spec §7.13, §12).

Every state transition and action in the system is recorded as an
immutable ``AuditEntry``.  This module provides a single entry point
for creating and persisting entries.

Design rule from the spec: "Audit everything.  Append-only log of:
proposed action, decision, who/when, and outcome."
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from schemas import (
    ActorType,
    AuditEntry,
    AuditEvent,
)


def log_event(
    store,
    event: AuditEvent,
    incident_id: Optional[str] = None,
    detail: Optional[Dict[str, Any]] = None,
    actor: ActorType = ActorType.system,
) -> AuditEntry:
    """Create and persist an audit log entry.

    Parameters
    ----------
    store:
        The ``SentinelStore`` (or any object exposing
        ``save_audit_entry(entry)``).
    event:
        The audit event type (e.g. ``incident_created``,
        ``action_executed``).
    incident_id:
        The related incident ID, if any.
    detail:
        Event-specific payload — a free-form dict that captures context
        (e.g. the action target, resolution reason, outcome values).
    actor:
        Who/what triggered this event (``system`` or ``human``).

    Returns
    -------
    AuditEntry
        The newly created, persisted audit entry.
    """
    entry = AuditEntry(
        entry_id=str(uuid.uuid4()),
        ts=datetime.utcnow(),
        incident_id=incident_id,
        event=event,
        actor=actor,
        detail=detail or {},
    )
    store.save_audit(entry)
    return entry
