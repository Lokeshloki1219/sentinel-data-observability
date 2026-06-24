"""End-to-end smoke test for the Sentinel control loop (no LLM required).

Run with:  python tests/smoke_test.py   (or  pytest tests/smoke_test.py)
"""

import sys
sys.path.insert(0, ".")

from pipeline.flows import run_pipeline
from pipeline.faults import FaultSpec
from observability.store import SentinelStore
from intent.parser import load_intent
from memory.store import MemoryStore
from orchestrator import process_run


def test_smoke_control_loop() -> None:
    store = SentinelStore(":memory:")
    memory = MemoryStore("data/test_chroma_smoke")
    intent = load_intent("transactions")

    # Phase 1: clean baseline — should produce no incidents.
    print("=== Clean baseline runs ===")
    clean_incident_total = 0
    for day in range(6):
        manifest = run_pipeline(day, num_rows=5000)
        incidents = process_run(manifest, store, intent, memory_store=memory,
                                reporter=None, auto_resolve=False)
        clean_incident_total += len(incidents)
        print(f"  Day {day}: run_id={manifest['run_id']}, incidents={len(incidents)}")
    assert clean_incident_total == 0, f"Clean runs raised {clean_incident_total} incidents"

    # Phase 2: inject a row-drop fault — should raise a volume incident.
    print("\n=== Fault injection: row_drop 60% ===")
    fault = FaultSpec(fault_type="row_drop", target="", params={"drop_pct": 0.6}, seed=7)
    manifest = run_pipeline(6, fault_spec=fault, num_rows=5000)
    incidents = process_run(manifest, store, intent, memory_store=memory,
                            reporter=None, auto_resolve=False)
    print(f"  run_id={manifest['run_id']}, incidents={len(incidents)}")
    for inc in incidents:
        metrics = [a.metric for a in inc.anomalies]
        print(f"    incident {inc.incident_id}: status={inc.status.value}, metrics={metrics}")

    assert incidents, "Expected at least one incident from the row_drop fault"
    assert any(
        a.check_type.value == "volume"
        for inc in incidents for a in inc.anomalies
    ), "Expected a volume anomaly"

    # Incidents must be persisted and retrievable.
    persisted = store.get_open_incidents()
    assert persisted, "Incident was not persisted to the store"

    # Freshness must be observable (P0-1): stale data should exceed the SLA.
    print("\n=== Fault injection: stale_data 10 days ===")
    stale = FaultSpec(fault_type="stale_data", target="", params={"stale_days": 10}, seed=8)
    manifest = run_pipeline(4, fault_spec=stale, num_rows=5000)
    incidents = process_run(manifest, store, intent, memory_store=memory,
                            reporter=None, auto_resolve=False)
    assert any(
        a.check_type.value == "freshness"
        for inc in incidents for a in inc.anomalies
    ), "Expected a freshness anomaly from stale_data (P0-1 regression)"
    print(f"  freshness incident raised: {bool(incidents)}")

    print("\n=== All smoke checks passed! ===")


if __name__ == "__main__":
    test_smoke_control_loop()
