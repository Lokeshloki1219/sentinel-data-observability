"""Extended smoke test covering memory, reasoning, action, and governance modules."""

import sys
sys.path.insert(0, ".")

import uuid
from datetime import datetime

from pipeline.flows import run_pipeline
from pipeline.faults import FaultSpec
from observability.store import SentinelStore
from observability.metrics import compute_metrics
from observability.detection.engine import run_detection, group_related
from intent.parser import load_intent
from schemas import (
    ActionType, AuditEvent, DecisionType, GateType, Incident, IncidentStatus,
    MemoryRecord, ReasonCode, Resolution, SeverityLevel
)
from memory.store import MemoryStore
from memory.embed import build_summary_text
from memory.retrieve import retrieve_similar
from reasoning.context import assemble_context
from action.registry import get_action, ACTION_REGISTRY
from action.preview import preview_action
from action.executor import ActionExecutor
from governance.policy import evaluate_gate
from governance.audit import log_event
from governance.suppression import create_suppression_from_incident, matches_suppression
from governance.approval import process_resolution
from governance.resolution import check_auto_resolution

store = SentinelStore(":memory:")
intent = load_intent("transactions")

# Build baseline
print("=== Building baseline (3 clean runs) ===")
for day in range(3):
    result = run_pipeline(day, num_rows=500)
    for stage, batch in result["batches"].items():
        m = compute_metrics(result["run_id"], "transactions", stage, batch, intent.key_columns)
        store.save_metrics(m)
    for _, op in result["operational_signals"].items():
        store.save_ops_signals(op)
print("  Baseline built.")

# Fault injection
print("\n=== Injecting distribution_shift fault ===")
fault = FaultSpec(fault_type="distribution_shift", target="amount", params={"factor": 10.0})
result = run_pipeline(3, fault_spec=fault, num_rows=500)

all_anomalies = []
for stage, batch in result["batches"].items():
    m = compute_metrics(result["run_id"], "transactions", stage, batch, intent.key_columns)
    store.save_metrics(m)
    anomalies = run_detection(m, store, intent)
    all_anomalies.extend(anomalies)

print(f"  Detected {len(all_anomalies)} anomalies total")

# Create incident
groups = group_related(all_anomalies)
incident = Incident(
    incident_id=str(uuid.uuid4())[:12],
    created_at=datetime.utcnow(),
    dataset="transactions",
    stage=groups[0][0].stage if groups else "unknown",
    run_id=result["run_id"],
    anomalies=groups[0] if groups else all_anomalies[:1],
    status=IncidentStatus.open,
)
store.save_incident(incident)
print(f"  Created incident: {incident.incident_id}")

# Audit log
entry = log_event(store, AuditEvent.incident_created, incident.incident_id, {"anomaly_count": len(all_anomalies)})
print(f"  Audit entry: {entry.entry_id}")

# Reasoning context (without LLM call)
print("\n=== Testing reasoning context assembly ===")
recent = store.get_recent_metrics("transactions", incident.stage, n=10)
ops = store.get_ops_signals(result["run_id"])
ctx = assemble_context(
    anomalies=incident.anomalies,
    intent=intent,
    recent_metrics=recent,
    operational=ops,
    schema_current=[],
    similar_incidents=[],
)
print(f"  Context assembled: {len(ctx.recent_metrics)} recent metrics, {len(ctx.operational)} ops signals")

# Action registry
print("\n=== Testing action layer ===")
action_def = get_action(ActionType.rerun_job)
print(f"  rerun_job: risk={action_def.risk_tier.value}, gate={action_def.gate.value}")
preview = preview_action(ActionType.rerun_job, "enriched", "transactions")
print(f"  Preview: {preview.description}")

# Gate evaluation
gate = evaluate_gate(action_def, intent.criticality)
print(f"  Gate for high-criticality dataset: {gate.value}")

# Executor (rerun_job — simulated)
executor = ActionExecutor(store)
exec_result = executor.execute(ActionType.rerun_job, "enriched", result["run_id"])
print(f"  Execute result: success={exec_result['success']}")

# Governance: suppression
print("\n=== Testing governance ===")
rule = create_suppression_from_incident(incident, store)
print(f"  Suppression rule created: {rule.rule_id}")
rules = store.get_active_suppressions("transactions")
if all_anomalies:
    is_suppressed = matches_suppression(all_anomalies[0], rules)
    print(f"  Anomaly suppressed by rule: {is_suppressed}")

# Test approval routing
print("\n=== Testing approval routing ===")
# Create a fresh incident for approval
incident2 = Incident(
    incident_id=str(uuid.uuid4())[:12],
    created_at=datetime.utcnow(),
    dataset="transactions",
    stage="raw_transactions",
    run_id=result["run_id"],
    anomalies=all_anomalies[:1] if all_anomalies else [],
    status=IncidentStatus.open,
)
store.save_incident(incident2)

# Test "not_a_problem" route
resolution = Resolution(
    incident_id=incident2.incident_id,
    decision=DecisionType.rejected,
    reason=ReasonCode.not_a_problem,
)

# We need a memory store for approval routing
import tempfile, os
tmpdir = os.path.join(".", "data", "test_chroma")
os.makedirs(tmpdir, exist_ok=True)
mem_store = MemoryStore(tmpdir)

updated = process_resolution(resolution, incident2, store, mem_store)
print(f"  Incident after 'not_a_problem': status={updated.status.value}")

print()
print("=== ALL EXTENDED TESTS PASSED ===")
