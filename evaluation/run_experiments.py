"""
Sentinel — Evaluation: Experiment Runner (§15).

End-to-end evaluation harness driven by the real control-loop
:func:`orchestrator.process_run`.  Generates clean and faulted runs, then
measures detection precision/recall/F1, root-cause attribution, report
quality, the memory-ablation effect, the false-positive trend, and clean-run
robustness.

Phases
------
1. **Clean baseline** — builds the rolling baseline and measures the
   false-positive rate on healthy runs.
2. **Memory bootstrap** — replays each fault on a throwaway store and writes
   the resolved incidents into the shared vector memory, so the measured
   phase has a corpus to retrieve from (spec §15: "Bootstrap Memory first").
3. **Measured faults** — injects each labelled fault on the clean baseline
   store and records detection + attribution + ablation.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure project root is on path
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from config import config
from schemas import Incident, MemoryRecord, Outcome, ResolutionMethod
from pipeline.flows import run_pipeline
from pipeline.faults import FaultSpec
from observability.store import SentinelStore
from intent.parser import load_intent
from memory.store import MemoryStore
from memory.embed import build_summary_text
from reasoning.context import assemble_context
from reasoning.reporter import Reporter
from orchestrator import process_run
from evaluation.detection_metrics import (
    DetectionResult,
    evaluate_detection,
    compute_detection_latency,
    compute_fp_trend,
)
from evaluation.attribution import evaluate_attribution
from evaluation.report_rubric import evaluate_report_quality
from evaluation.memory_ablation import run_memory_ablation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("evaluation")


# ── Fault scenarios with expected caused_by ──────────────────────────────

FAULT_SCENARIOS: List[Tuple[FaultSpec, str]] = [
    (FaultSpec(fault_type="row_drop", target="", params={"drop_pct": 0.5}, seed=1), "data_source"),
    (FaultSpec(fault_type="column_null", target="amount", params={"null_pct": 0.3}, seed=2), "data_source"),
    (FaultSpec(fault_type="schema_change", target="oldbalanceOrg", params={"action": "drop"}, seed=3), "schema_change"),
    (FaultSpec(fault_type="distribution_shift", target="amount", params={"factor": 10.0}, seed=4), "pipeline_logic"),
    (FaultSpec(fault_type="stale_data", target="", params={"stale_days": 7}, seed=5), "data_source"),
    (FaultSpec(
        fault_type="operational_cause",
        target="enriched",
        params={
            "job_status": "failed",
            "downstream_fault_type": "row_drop",
            "downstream_target": "rows",
            "downstream_params": {"drop_pct": 0.85},  # failed upstream → heavy data loss
        },
        seed=6,
    ), "upstream_job"),
    # ── extended coverage ──
    (FaultSpec(fault_type="duplicate_rows", target="", params={"dup_pct": 0.3}, seed=7), "pipeline_logic"),
    (FaultSpec(fault_type="out_of_range", target="amount", params={"pct": 0.1, "value": -1.0}, seed=8), "data_source"),
    (FaultSpec(fault_type="oom", target="enriched", params={}, seed=9), "infrastructure"),
    (FaultSpec(fault_type="timeout", target="enriched", params={}, seed=10), "infrastructure"),
    (FaultSpec(fault_type="retry_storm", target="cleaned_typed", params={"retries": 5}, seed=11), "infrastructure"),
]


def _fresh_paths() -> Tuple[str, str]:
    """Return isolated (db_path, chroma_dir) for a reproducible eval run."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    db = config.DATA_DIR / "eval.duckdb"
    chroma = config.DATA_DIR / "eval_chroma"
    for p in (db,):
        if p.exists():
            p.unlink()
    if chroma.exists():
        import shutil

        shutil.rmtree(chroma, ignore_errors=True)
    return str(db), str(chroma)


def _bootstrap_memory(
    memory_store: MemoryStore,
    intent,
    reporter: Optional[Reporter],
    start_day: int,
) -> int:
    """Replay each fault on a throwaway store and seed memory (spec §15).

    Returns the number of MemoryRecords written.  Uses an in-memory store so
    the measured-phase baseline is never polluted by bootstrap metrics.
    """
    throwaway = SentinelStore(":memory:")
    written = 0
    for i, (fault_spec, _cause) in enumerate(FAULT_SCENARIOS):
        manifest = run_pipeline(start_day + i, fault_spec=fault_spec)
        incidents = process_run(
            manifest, throwaway, intent,
            memory_store=memory_store, reporter=reporter,
            auto_resolve=False,
        )
        for inc in incidents:
            # Synthesize a resolved outcome so the memory record is a complete
            # positive exemplar (anomaly + cause + fix worked).
            inc.outcome = Outcome(
                incident_id=inc.incident_id,
                resolved=True,
                resolved_at=datetime.now(timezone.utc),
                time_to_resolution_minutes=0.0,
                resolution_method=ResolutionMethod.auto,
                fix_worked=True,
            )
            record = MemoryRecord(
                incident_id=inc.incident_id,
                dataset=inc.dataset,
                check_type=inc.anomalies[0].check_type.value if inc.anomalies else "unknown",
                summary_text=build_summary_text(inc),
                report=inc.report,
                outcome=inc.outcome,
            )
            memory_store.add_record(record)
            written += 1
    logger.info("Memory bootstrap complete: %d records written.", written)
    return written


def run_experiments(
    num_clean_runs: int = 10,
    use_llm: bool = False,
    output_path: Optional[str] = None,
) -> Dict:
    """Run the full evaluation protocol via the real orchestrator."""
    db_path, chroma_dir = _fresh_paths()
    store = SentinelStore(db_path)
    memory_store = MemoryStore(chroma_dir)
    intent = load_intent("transactions")
    reporter = Reporter() if use_llm else None

    results: Dict = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": {
            "num_clean_runs": num_clean_runs,
            "use_llm": use_llm,
            "fault_scenarios": len(FAULT_SCENARIOS),
        },
    }

    # ── Phase 1: Clean baseline ─────────────────────────────────────────
    logger.info("=== Phase 1: Clean baseline runs (%d) ===", num_clean_runs)
    clean_fp_counts: List[int] = []
    clean_runs_detected: List[List[str]] = []
    for day in range(num_clean_runs):
        manifest = run_pipeline(day)
        incidents = process_run(
            manifest, store, intent, memory_store=memory_store, reporter=None,
            auto_resolve=False,
        )
        fp_metrics = [a.metric for inc in incidents for a in inc.anomalies]
        clean_fp_counts.append(len(fp_metrics))
        clean_runs_detected.append(fp_metrics)
    logger.info("Clean runs complete. FP counts per run: %s", clean_fp_counts)

    # ── Phase 2: Memory bootstrap ───────────────────────────────────────
    if use_llm:
        logger.info("=== Phase 2: Memory bootstrap ===")
        _bootstrap_memory(memory_store, intent, reporter, start_day=1000)

    # ── Phase 3: Measured fault injection ───────────────────────────────
    logger.info("=== Phase 3: Fault injection (%d scenarios) ===", len(FAULT_SCENARIOS))
    detection_results: List[DetectionResult] = []
    ground_truths: List[dict] = []
    reports_with_memory: List[Optional[dict]] = []
    reports_without_memory: List[Optional[dict]] = []

    for i, (fault_spec, expected_cause) in enumerate(FAULT_SCENARIOS):
        day = num_clean_runs + i
        logger.info("--- Scenario %d: %s ---", i + 1, fault_spec.fault_type)
        manifest = run_pipeline(day, fault_spec=fault_spec)

        incidents = process_run(
            manifest, store, intent, memory_store=memory_store, reporter=reporter,
            auto_resolve=False,
        )

        # Ground truth
        gt_label = manifest.get("fault_label") or {
            "fault_type": fault_spec.fault_type,
            "target": fault_spec.target,
            "params": fault_spec.params,
        }
        gt_label["caused_by"] = gt_label.get("caused_by", expected_cause)
        gt_label["run_id"] = manifest["run_id"]
        ground_truths.append(gt_label)

        detected_metrics = [a.metric for inc in incidents for a in inc.anomalies]
        detected_checks = [a.check_type.value for inc in incidents for a in inc.anomalies]
        detection_results.append(DetectionResult(
            run_id=manifest["run_id"],
            injected_fault_type=fault_spec.fault_type,
            injected_target=fault_spec.target,
            detected_anomalies=detected_metrics,
            detected_checks=detected_checks,
            was_detected=len(detected_metrics) > 0,
        ))

        # Reports for attribution + ablation (LLM only)
        with_mem = incidents[0].report.model_dump() if (incidents and incidents[0].report) else None
        reports_with_memory.append(with_mem)

        without_mem = None
        if reporter is not None and incidents and incidents[0].context_used is not None:
            ctx = incidents[0].context_used
            ablate_ctx = assemble_context(
                anomalies=[ctx.anomaly],
                intent=intent,
                recent_metrics=ctx.recent_metrics,
                operational=ctx.operational,
                schema_current=ctx.schema_current,
                similar_incidents=[],  # ablation: no memory
            )
            rep_no_mem, _ = reporter.generate_report(ablate_ctx)
            without_mem = rep_no_mem.model_dump() if rep_no_mem else None
        reports_without_memory.append(without_mem)

    # ── Phase 4: Metrics ────────────────────────────────────────────────
    logger.info("=== Phase 4: Computing evaluation metrics ===")
    det = evaluate_detection(detection_results, clean_runs_detected)
    results["detection"] = {
        "precision": round(det.precision, 4),
        "recall": round(det.recall, 4),
        "f1": round(det.f1, 4),
        "true_positives": det.true_positives,
        "false_positives": det.false_positives,
        "false_negatives": det.false_negatives,
        "per_scenario": [
            {"fault": r.injected_fault_type, "matched": r.matched,
             "any_anomaly": r.was_detected, "checks": sorted(set(r.detected_checks))}
            for r in detection_results
        ],
    }

    # Detection latency (Spec §15): runs from fault occurrence to escalation.
    # Each fault is injected on one run and escalates in that run (0) when
    # detected; undetected scenarios are excluded.
    latency_by_fault = compute_detection_latency(
        [(r.injected_fault_type, 0) for r in detection_results if r.was_detected]
    )
    results["detection"]["latency_by_fault_runs"] = latency_by_fault
    results["detection"]["mean_latency_runs"] = (
        round(sum(latency_by_fault.values()) / len(latency_by_fault), 3)
        if latency_by_fault else None
    )

    if use_llm:
        attr = evaluate_attribution(ground_truths, reports_with_memory)
        results["attribution"] = {
            "overall_accuracy": round(attr["overall_accuracy"], 4),
            "operational_cause_accuracy": round(attr["operational_cause_accuracy"], 4),
            "per_cause": attr["per_cause_accuracy"],
        }
        if any(r is not None for r in reports_with_memory):
            quality = evaluate_report_quality(reports_with_memory, ground_truths)
            results["report_quality"] = {
                k: (round(v, 4) if isinstance(v, float) else v)
                for k, v in quality.items() if k != "scores"
            }
            ablation = run_memory_ablation(
                reports_with_memory, reports_without_memory, ground_truths
            )
            results["memory_ablation"] = {
                "with_memory": ablation["with_memory"],
                "without_memory": ablation["without_memory"],
                "deltas": ablation["deltas"],
                "memory_helps": ablation["memory_helps"],
            }
    else:
        results["attribution"] = "skipped (run with --use-llm)"
        results["memory_ablation"] = "skipped (run with --use-llm)"

    results["fp_trend"] = compute_fp_trend(clean_fp_counts)
    total_clean = max(len(clean_fp_counts), 1)
    fp_runs = sum(1 for c in clean_fp_counts if c > 0)
    results["robustness"] = {
        "clean_run_fp_rate": round(fp_runs / total_clean, 4),
        "total_clean_runs": total_clean,
    }

    # ── Phase 5: graduated degradation + real suppression loop (no LLM) ──
    # The credible picture: same detector, faults across magnitudes on a
    # baseline with realistic variance, plus the reject->suppression->FP-drop
    # loop actually exercised.
    logger.info("=== Phase 5: Graduated degradation + suppression loop ===")
    from evaluation.graduated import run_graduated, run_suppression_demo
    grad = run_graduated(output_path="data/graduated_eval.json", verbose=False)
    supp = run_suppression_demo(output_path="data/suppression_demo.json", verbose=False)
    results["graduated"] = {
        "threshold_sweep": grad["threshold_sweep"],
        "per_severity_recall": grad["per_severity_recall"],
        "best_f1": grad["best_f1"],
        "volume_detection": grad["graduated_detection"]["volume"],
    }
    results["suppression_loop"] = supp

    # ── Output ──────────────────────────────────────────────────────────
    logger.info("=== Results ===\n%s", json.dumps(results, indent=2, default=str))
    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        logger.info("Results saved to %s", output_path)

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sentinel Evaluation Runner")
    parser.add_argument("--clean-runs", type=int, default=10)
    parser.add_argument("--use-llm", action="store_true")
    parser.add_argument("--output", type=str, default="data/eval_results.json")
    args = parser.parse_args()

    run_experiments(
        num_clean_runs=args.clean_runs,
        use_llm=args.use_llm,
        output_path=args.output,
    )
