"""
Sentinel — Evaluation: Root-Cause Attribution (§15).

Measures the fraction of operational-cause faults where the LLM
correctly identifies `caused_by = upstream_job`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class AttributionResult:
    """One attribution evaluation case."""
    run_id: str
    ground_truth_caused_by: str   # from fault injection label
    predicted_caused_by: str      # from ReasoningOutput.caused_by
    correct: bool


def evaluate_attribution(
    ground_truths: List[dict],
    predictions: List[Optional[dict]],
) -> dict:
    """Evaluate root-cause attribution accuracy.

    Args:
        ground_truths: List of fault injection labels, each containing
            {'fault_type', 'target', 'params', 'caused_by'}.
        predictions: List of ReasoningOutput dicts (or None if report_invalid),
            one per ground truth. Each contains 'caused_by'.

    Returns:
        Dict with overall accuracy and per-cause-type breakdown.
    """
    results: List[AttributionResult] = []

    for gt, pred in zip(ground_truths, predictions):
        gt_cause = gt.get("caused_by", "unknown")

        if pred is None:
            pred_cause = "report_invalid"
        else:
            pred_cause = pred.get("caused_by", "unknown")

        results.append(AttributionResult(
            run_id=gt.get("run_id", ""),
            ground_truth_caused_by=gt_cause,
            predicted_caused_by=pred_cause,
            correct=(gt_cause == pred_cause),
        ))

    total = len(results)
    correct = sum(1 for r in results if r.correct)

    # Per-cause breakdown
    from collections import defaultdict
    per_cause: dict = defaultdict(lambda: {"total": 0, "correct": 0})
    for r in results:
        per_cause[r.ground_truth_caused_by]["total"] += 1
        if r.correct:
            per_cause[r.ground_truth_caused_by]["correct"] += 1

    per_cause_accuracy = {
        cause: stats["correct"] / stats["total"] if stats["total"] > 0 else 0.0
        for cause, stats in per_cause.items()
    }

    # Special metric: operational-cause attribution accuracy
    op_results = [r for r in results if r.ground_truth_caused_by == "upstream_job"]
    op_correct = sum(1 for r in op_results if r.correct)
    op_accuracy = op_correct / len(op_results) if op_results else 0.0

    return {
        "overall_accuracy": correct / total if total > 0 else 0.0,
        "total_cases": total,
        "correct_cases": correct,
        "per_cause_accuracy": dict(per_cause_accuracy),
        "operational_cause_accuracy": op_accuracy,
        "operational_cause_cases": len(op_results),
        "results": results,
    }
