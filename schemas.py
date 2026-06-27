"""
Sentinel — Normative Data Schemas (Section 7 of Technical Spec).

All Pydantic models defined here are the single source of truth for
field names, types, and semantics.  Every module imports from here.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    """Timezone-aware UTC now (replaces the deprecated ``datetime.utcnow``)."""
    return datetime.now(timezone.utc)


# ── Enums ──────────────────────────────────────────────────────────────────

class Criticality(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class JobStatus(str, Enum):
    success = "success"
    failed = "failed"
    skipped = "skipped"
    running = "running"
    retrying = "retrying"


class CheckType(str, Enum):
    freshness = "freshness"
    volume = "volume"
    schema = "schema"
    null_rate = "null_rate"
    distribution = "distribution"
    validity = "validity"          # value out of plausible range
    uniqueness = "uniqueness"      # duplicate rows on a key
    operational = "operational"    # job failed/slow/retried (OOM, timeout, …)


class SeverityLevel(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


class CausedBy(str, Enum):
    data_source = "data_source"
    upstream_job = "upstream_job"
    schema_change = "schema_change"
    pipeline_logic = "pipeline_logic"
    infrastructure = "infrastructure"   # OOM / timeout / compute / resource exhaustion
    unknown = "unknown"


class ActionType(str, Enum):
    rerun_job = "rerun_job"
    quarantine_batch = "quarantine_batch"
    backfill = "backfill"
    none = "none"
    manual = "manual"


class RiskTier(str, Enum):
    safe = "safe"
    medium = "medium"
    risky = "risky"
    blocked = "blocked"


class GateType(str, Enum):
    one_click = "one_click"
    typed_confirmation = "typed_confirmation"
    blocked = "blocked"


class IncidentStatus(str, Enum):
    open = "open"
    awaiting_approval = "awaiting_approval"
    acted = "acted"
    resolved = "resolved"
    suppressed = "suppressed"
    acknowledged_manual = "acknowledged_manual"
    snoozed = "snoozed"
    rejected = "rejected"
    report_invalid = "report_invalid"


class DecisionType(str, Enum):
    approved = "approved"
    modified = "modified"
    rejected = "rejected"
    snoozed = "snoozed"


class ReasonCode(str, Enum):
    none = "none"
    not_a_problem = "not_a_problem"
    will_fix_manually = "will_fix_manually"
    wrong_diagnosis = "wrong_diagnosis"
    defer = "defer"


class ResolutionMethod(str, Enum):
    action = "action"
    manual = "manual"
    auto = "auto"
    none = "none"


class SuppressionEffect(str, Enum):
    suppress = "suppress"
    raise_threshold = "raise_threshold"


class AuditEvent(str, Enum):
    anomaly_detected = "anomaly_detected"
    incident_created = "incident_created"
    report_generated = "report_generated"
    gate_evaluated = "gate_evaluated"
    action_proposed = "action_proposed"
    resolution_recorded = "resolution_recorded"
    action_executed = "action_executed"
    action_undone = "action_undone"
    outcome_recorded = "outcome_recorded"
    suppression_created = "suppression_created"


class ActorType(str, Enum):
    system = "system"
    human = "human"


# ── 7.1  IntentConfig ──────────────────────────────────────────────────────

class ExpectedVolume(BaseModel):
    min_rows: int
    max_rows: int


class ColumnRange(BaseModel):
    min: float
    max: float


class IntentConfig(BaseModel):
    dataset: str
    owner: str
    consumers: List[str] = Field(default_factory=list)
    criticality: Criticality = Criticality.medium
    expected_schedule_cron: str = ""
    freshness_sla_minutes: int = 60
    key_columns: List[str] = Field(default_factory=list)
    accepted_null_pct: Dict[str, float] = Field(default_factory=dict)
    expected_volume: Optional[ExpectedVolume] = None
    # ── extended coverage (all optional; checks only fire when configured) ──
    expected_ranges: Dict[str, ColumnRange] = Field(default_factory=dict)  # validity
    unique_key: List[str] = Field(default_factory=list)                    # uniqueness
    max_duration_seconds: Optional[float] = None                           # operational: timeout
    max_retries: Optional[int] = None                                      # operational: retry storm


# ── 7.2  RunMetrics ───────────────────────────────────────────────────────

class ColumnSchema(BaseModel):
    name: str
    dtype: str


class NumericStats(BaseModel):
    mean: float
    std: float
    p05: float
    p50: float
    p95: float
    min: float
    max: float


class RunMetrics(BaseModel):
    run_id: str
    dataset: str
    stage: str
    ts_run: datetime
    event_time_max: datetime
    row_count: int
    freshness_minutes: float
    schema_hash: str
    schema_: List[ColumnSchema] = Field(alias="schema")
    null_rate: Dict[str, float] = Field(default_factory=dict)
    numeric_stats: Dict[str, NumericStats] = Field(default_factory=dict)
    categorical_dist: Dict[str, Dict[str, float]] = Field(default_factory=dict)
    duplicate_rate: float = 0.0    # fraction of rows duplicated on the uniqueness key

    model_config = {"populate_by_name": True}

    @staticmethod
    def compute_schema_hash(columns: List[ColumnSchema]) -> str:
        """Stable hash of ordered [(name, dtype)]."""
        canonical = json.dumps(
            [(c.name, c.dtype) for c in sorted(columns, key=lambda c: c.name)],
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]


# ── 7.3  OperationalSignals ───────────────────────────────────────────────

class OperationalSignals(BaseModel):
    run_id: str
    job_name: str
    status: JobStatus
    started_at: datetime
    ended_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    retries: int = 0
    exit_code: Optional[int] = None
    upstream_jobs: List[str] = Field(default_factory=list)


# ── 7.4  Anomaly ──────────────────────────────────────────────────────────

class Anomaly(BaseModel):
    anomaly_id: str
    run_id: str
    dataset: str
    stage: str
    metric: str
    check_type: CheckType
    observed: Any  # number | str
    expected: Any  # number | str | {min, max}
    deviation: float
    severity_hint: SeverityLevel
    detected_at: datetime
    escalated: bool = False


# ── 7.5  ReasoningContext ─────────────────────────────────────────────────

class MemoryRecord(BaseModel):
    """Forward-declared here so ReasoningContext can reference it."""
    incident_id: str
    dataset: str
    check_type: str
    summary_text: str
    embedding: Optional[List[float]] = None  # vector
    report: Optional[ReasoningOutput] = None  # forward ref resolved below
    outcome: Optional[Outcome] = None        # forward ref resolved below


class ReasoningContext(BaseModel):
    anomaly: Anomaly
    intent: IntentConfig
    recent_metrics: List[RunMetrics] = Field(default_factory=list)
    operational: List[OperationalSignals] = Field(default_factory=list)
    schema_current: List[ColumnSchema] = Field(default_factory=list)
    code_version: str = ""
    similar_incidents: List[MemoryRecord] = Field(default_factory=list)


# ── 7.6  ReasoningOutput ─────────────────────────────────────────────────

class SuggestedAction(BaseModel):
    type: ActionType
    target: str = ""
    rationale: str = ""


class ReasoningOutput(BaseModel):
    severity: SeverityLevel
    likely_root_cause: str
    caused_by: CausedBy
    evidence: List[str] = Field(default_factory=list)
    suggested_action: SuggestedAction
    confidence: float = Field(ge=0.0, le=1.0)


# ── 7.7  Incident ────────────────────────────────────────────────────────

class Incident(BaseModel):
    incident_id: str
    created_at: datetime
    dataset: str
    stage: str
    run_id: str
    anomalies: List[Anomaly] = Field(default_factory=list)
    context_used: Optional[ReasoningContext] = None
    report: Optional[ReasoningOutput] = None
    status: IncidentStatus = IncidentStatus.open
    resolution: Optional[Resolution] = None  # forward ref resolved below
    outcome: Optional[Outcome] = None        # forward ref resolved below
    embedding_id: Optional[str] = None


# ── 7.8  ActionDefinition ────────────────────────────────────────────────

class ActionDefinition(BaseModel):
    action_type: ActionType
    risk_tier: RiskTier
    reversible: bool
    gate: GateType
    # preview / execute / undo are runtime callables, not serialized.


# ── 7.9  Resolution ──────────────────────────────────────────────────────

class Resolution(BaseModel):
    incident_id: str
    decision: DecisionType
    reason: ReasonCode = ReasonCode.none
    modified_action: Optional[ActionDefinition] = None
    manual_fix_note: Optional[str] = None
    decided_by: str = "human"
    decided_at: datetime = Field(default_factory=_utcnow)


# ── 7.10 Outcome ─────────────────────────────────────────────────────────

class Outcome(BaseModel):
    incident_id: str
    resolved: bool
    resolved_at: Optional[datetime] = None
    time_to_resolution_minutes: Optional[float] = None
    resolution_method: ResolutionMethod = ResolutionMethod.none
    fix_worked: Optional[bool] = None


# ── 7.11 SuppressionRule ─────────────────────────────────────────────────

class SuppressionMatch(BaseModel):
    metric: str
    check_type: str
    condition: str = ""


class SuppressionRule(BaseModel):
    rule_id: str
    dataset: str
    match: SuppressionMatch
    effect: SuppressionEffect = SuppressionEffect.suppress
    param: Optional[float] = None
    created_from_incident: str
    created_at: datetime = Field(default_factory=_utcnow)


# ── 7.13 AuditEntry ──────────────────────────────────────────────────────

class AuditEntry(BaseModel):
    entry_id: str
    ts: datetime = Field(default_factory=_utcnow)
    incident_id: Optional[str] = None
    event: AuditEvent
    actor: ActorType = ActorType.system
    detail: Dict[str, Any] = Field(default_factory=dict)


# ── Forward-reference resolution ─────────────────────────────────────────
# Pydantic v2 deferred annotations require explicit model_rebuild().

MemoryRecord.model_rebuild()
Incident.model_rebuild()
