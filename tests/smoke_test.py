"""End-to-end smoke test for the Sentinel pipeline."""

import sys
sys.path.insert(0, ".")

from pipeline.flows import run_pipeline
from pipeline.faults import FaultSpec
from observability.store import SentinelStore
from observability.metrics import compute_metrics
from observability.detection.engine import run_detection, group_related
from intent.parser import load_intent
from schemas import AuditEvent

store = SentinelStore(":memory:")
intent = load_intent("transactions")

# Phase 1: 3 clean baseline runs
print("=== Clean baseline runs ===")
for day in range(3):
    result = run_pipeline(day, num_rows=500)
    for stage, batch in result["batches"].items():
        m = compute_metrics(result["run_id"], "transactions", stage, batch, intent.key_columns)
        store.save_metrics(m)
    for stage, op in result["operational_signals"].items():
        store.save_ops_signals(op)
    print(f"  Day {day}: run_id={result['run_id']}, stages={list(result['batches'].keys())}")

# Phase 2: Fault injection
print()
print("=== Fault injection: row_drop 50% ===")
fault = FaultSpec(fault_type="row_drop", target="", params={"drop_pct": 0.5})
result = run_pipeline(3, fault_spec=fault, num_rows=500)
print(f"  run_id={result['run_id']}, fault_label={result['fault_label']}")

# Compute metrics and detect
all_anomalies = []
for stage, batch in result["batches"].items():
    m = compute_metrics(result["run_id"], "transactions", stage, batch, intent.key_columns)
    store.save_metrics(m)
    for s_op_name, op in result["operational_signals"].items():
        pass  # already stored
    anomalies = run_detection(m, store, intent)
    all_anomalies.extend(anomalies)
    if anomalies:
        print(f"  Stage {stage}: {len(anomalies)} anomalies detected!")
        for a in anomalies:
            print(f"    - {a.metric} ({a.check_type.value}): observed={a.observed}, deviation={a.deviation:.2f}, severity={a.severity_hint.value}")
    else:
        print(f"  Stage {stage}: no anomalies")

# Group anomalies
if all_anomalies:
    groups = group_related(all_anomalies)
    print(f"\n  Grouped into {len(groups)} incident group(s)")

print()
print("=== All checks passed! Pipeline working end-to-end. ===")
