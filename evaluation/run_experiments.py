"""
Sentinel — Evaluation: Experiment Runner (§15).

End-to-end evaluation harness. Generates clean and faulted runs,
measures detection F1, attribution accuracy, report quality,
memory ablation, and false-positive trend.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Ensure project root is on path
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from config import config
from schemas import (
    ActionType,
    Anomaly,
    AuditEvent,
    Incident,
    IncidentStatus,
    MemoryRecord,
    ReasoningOutput,
)
from pipeline.ingest import generate_batch
from pipeline.faults import FaultSpec, inject_fault
from pipeline.transform.transform import stage_raw, stage_cleaned, stage_enriched, stage_fraud_features
from pipeline.flows import run_pipeline
from observability.store import SentinelStore
from observability.metrics import compute_metrics
from observability.operational import collect_signals
from observability.detection.engine import run_detection, group_related
from intent.parser import load_intent
from memory.store import MemoryStore
from memory.retrieve import retrieve_similar
from memory.embed import build_summary_text
from reasoning.context import assemble_context
from reasoning.reporter import Reporter
from governance.resolution import check_auto_resolution
from governance.audit import log_event
from evaluation.detection_metrics import (
    DetectionResult,
    DetectionMetrics,
    evaluate_detection,
    compute_fp_trend,
)
from evaluation.attribution import evaluate_attribution
from evaluation.report_rubric import evaluate_report_quality
from evaluation.memory_ablation import run_memory_ablation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("evaluation")


# ── Fault scenarios with expected caused_by ──────────────────────────────

# Each tuple is (FaultSpec, expected_caused_by_for_ground_truth)
FAULT_SCENARIOS: List[Tuple[FaultSpec, str]] = [
    (FaultSpec(fault_type="row_drop", target="", params={"drop_pct": 0.5}), "data_source"),
    (FaultSpec(fault_type="column_null", target="amount", params={"null_pct": 0.3}), "data_source"),
    (FaultSpec(fault_type="schema_change", target="oldbalanceOrg", params={"action": "drop"}), "schema_change"),
    (FaultSpec(fault_type="distribution_shift", target="amount", params={"factor": 10.0}), "pipeline_logic"),
    (FaultSpec(fault_type="stale_data", target="", params={"stale_days": 7}), "data_source"),
    (FaultSpec(fault_type="operational_cause", target="enriched", params={"job_status": "failed"}), "upstream_job"),
]


def run_experiments(
    num_clean_runs: int = 10,
    num_bootstrap_runs: int = 5,
    use_llm: bool = False,
    output_path: Optional[str] = None,
) -> Dict:
    """Run the full evaluation protocol.

    Args:
        num_clean_runs: Number of clean runs before fault injection.
        num_bootstrap_runs: Extra clean runs to bootstrap memory.
        use_llm: Whether to call the actual LLM (requires API key).
        output_path: Path to save results JSON.

    Returns:
        Dict with all evaluation results.
    """
    store = SentinelStore(config.DB_URL)
    memory_store = MemoryStore(str(config.CHROMA_DIR))
    intent = load_intent("transactions")
    reporter = Reporter() if use_llm else None

    results = {
        "timestamp": datetime.utcnow().isoformat(),
        "config": {
            "num_clean_runs": num_clean_runs,
            "num_bootstrap_runs": num_bootstrap_runs,
            "use_llm": use_llm,
            "fault_scenarios": len(FAULT_SCENARIOS),
        },
    }

    logger.info("=== Phase 1: Clean baseline runs (%d) ===", num_clean_runs + num_bootstrap_runs)
    clean_fp_counts: List[int] = []

    for day in range(num_clean_runs + num_bootstrap_runs):
        pipeline_result = run_pipeline(day)
        # Compute and store metrics for each stage
        for stage_name, batch in pipeline_result["batches"].items():
            metrics = compute_metrics(
                run_id=pipeline_result["run_id"],
                dataset="transactions",
                stage=stage_name,
                batch=batch,
                key_columns=intent.key_columns,
            )
            store.save_metrics(metrics)

            # Run detection
            anomalies = run_detection(metrics, store, intent)
            clean_fp_counts.append(len(anomalies))

        # Store operational signals
        for stage_name, op_sig in pipeline_result["operational_signals"].items():
            store.save_ops_signals(op_sig)

    logger.info("Clean runs complete. FP counts: %s", clean_fp_counts)

    # ── Phase 2: Fault injection ─────────────────────────────────────
    logger.info("=== Phase 2: Fault injection (%d scenarios) ===", len(FAULT_SCENARIOS))

    detection_results: List[DetectionResult] = []
    ground_truths: List[dict] = []
    reports_with_memory: List[Optional[dict]] = []
    reports_without_memory: List[Optional[dict]] = []

    for i, (fault_spec, expected_cause) in enumerate(FAULT_SCENARIOS):
        day = num_clean_runs + num_bootstrap_runs + i
        logger.info("--- Scenario %d: %s ---", i + 1, fault_spec.fault_type)

        pipeline_result = run_pipeline(day, fault_spec=fault_spec)

        # Build ground truth from the fault_label returned by the pipeline
        gt_label = pipeline_result.get("fault_label") or {
            "fault_type": fault_spec.fault_type,
            "target": fault_spec.target,
            "params": fault_spec.params,
        }
        # Ensure caused_by is set from our scenario definition
        gt_label["caused_by"] = gt_label.get("caused_by", expected_cause)
        gt_label["run_id"] = pipeline_result["run_id"]
        ground_truths.append(gt_label)

        # Store operational signals (including fault-injected upstream signals)
        for stage_name, op_sig in pipeline_result["operational_signals"].items():
            store.save_ops_signals(op_sig)
        if pipeline_result.get("fault_operational_signal"):
            store.save_ops_signals(pipeline_result["fault_operational_signal"])

        # Compute metrics and detect
        all_anomalies: List[Anomaly] = []
        for stage_name, batch in pipeline_result["batches"].items():
            metrics = compute_metrics(
                run_id=pipeline_result["run_id"],
                dataset="transactions",
                stage=stage_name,
                batch=batch,
                key_columns=intent.key_columns,
            )
            store.save_metrics(metrics)
            anomalies = run_detection(metrics, store, intent)
            all_anomalies.extend(anomalies)

        detection_results.append(DetectionResult(
            run_id=pipeline_result["run_id"],
            injected_fault_type=fault_spec.fault_type,
            injected_target=fault_spec.target,
            detected_anomalies=[a.metric for a in all_anomalies],
            was_detected=len(all_anomalies) > 0,
        ))

        # Reasoning (with memory)
        report_with: Optional[dict] = None
        report_without: Optional[dict] = None

        if all_anomalies and reporter:
            groups = group_related(all_anomalies)
            for group in groups[:1]:  # process the first group
                primary = group[0]
                # With memory
                similar = retrieve_similar(memory_store, primary, top_k=config.MEMORY_TOP_K)
                recent = store.get_recent_metrics(primary.dataset, primary.stage, n=10)
                ops = store.get_ops_signals(primary.run_id)
                ctx = assemble_context(
                    anomalies=group,
                    intent=intent,
                    recent_metrics=recent,
                    operational=ops,
                    schema_current=[],
                    similar_incidents=similar,
                )
                report_obj, valid = reporter.generate_report(ctx)
                if report_obj:
                    report_with = report_obj.model_dump()

                # Without memory (ablation)
                ctx_no_mem = assemble_context(
                    anomalies=group,
                    intent=intent,
                    recent_metrics=recent,
                    operational=ops,
                    schema_current=[],
                    similar_incidents=[],  # empty = ablation
                )
                report_no_mem, _ = reporter.generate_report(ctx_no_mem)
                if report_no_mem:
                    report_without = report_no_mem.model_dump()

        reports_with_memory.append(report_with)
        reports_without_memory.append(report_without)

    # ── Phase 3: Compute metrics ─────────────────────────────────────
    logger.info("=== Phase 3: Computing evaluation metrics ===")

    # Detection
    det_metrics = evaluate_detection(
        detection_results,
        [[]] * num_clean_runs,  # clean run FPs
    )
    results["detection"] = {
        "precision": round(det_metrics.precision, 4),
        "recall": round(det_metrics.recall, 4),
        "f1": round(det_metrics.f1, 4),
        "true_positives": det_metrics.true_positives,
        "false_positives": det_metrics.false_positives,
        "false_negatives": det_metrics.false_negatives,
    }

    # Attribution
    attr_results = evaluate_attribution(ground_truths, reports_with_memory)
    results["attribution"] = {
        "overall_accuracy": round(attr_results["overall_accuracy"], 4),
        "operational_cause_accuracy": round(attr_results["operational_cause_accuracy"], 4),
        "per_cause": attr_results["per_cause_accuracy"],
    }

    # Report quality
    if any(r is not None for r in reports_with_memory):
        quality = evaluate_report_quality(reports_with_memory, ground_truths)
        results["report_quality"] = {
            k: round(v, 4) if isinstance(v, float) else v
            for k, v in quality.items()
            if k != "scores"
        }

    # Memory ablation
    if any(r is not None for r in reports_with_memory):
        ablation = run_memory_ablation(reports_with_memory, reports_without_memory, ground_truths)
        results["memory_ablation"] = {
            k: v for k, v in ablation.items()
            if k not in ("with_memory", "without_memory")
        }
        results["memory_ablation"]["with_memory"] = ablation["with_memory"]
        results["memory_ablation"]["without_memory"] = ablation["without_memory"]

    # FP trend
    results["fp_trend"] = compute_fp_trend(clean_fp_counts)

    # Robustness (clean-run FP rate)
    total_clean_checks = max(len(clean_fp_counts), 1)
    fp_runs = sum(1 for c in clean_fp_counts if c > 0)
    results["robustness"] = {
        "clean_run_fp_rate": round(fp_runs / total_clean_checks, 4),
        "total_clean_runs": total_clean_checks,
    }

    # ── Output ───────────────────────────────────────────────────────
    logger.info("=== Results ===")
    logger.info(json.dumps(results, indent=2, default=str))

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
    parser.add_argument("--bootstrap-runs", type=int, default=5)
    parser.add_argument("--use-llm", action="store_true")
    parser.add_argument("--output", type=str, default="data/eval_results.json")
    args = parser.parse_args()

    run_experiments(
        num_clean_runs=args.clean_runs,
        num_bootstrap_runs=args.bootstrap_runs,
        use_llm=args.use_llm,
        output_path=args.output,
    )
