"""
Sentinel — Evaluation: Memory Ablation (§15).

Measures the report-quality delta with vs. without retrieved
incidents (memory ablation study).
"""

from __future__ import annotations

from typing import Dict, List, Optional
from evaluation.report_rubric import evaluate_report_quality


def run_memory_ablation(
    reports_with_memory: List[Optional[dict]],
    reports_without_memory: List[Optional[dict]],
    ground_truths: List[dict],
) -> Dict[str, float]:
    """Compare report quality with and without memory retrieval.

    Args:
        reports_with_memory: ReasoningOutput dicts generated with
            similar_incidents populated in the context.
        reports_without_memory: ReasoningOutput dicts generated with
            an empty similar_incidents list (ablation).
        ground_truths: Ground-truth fault labels for each case.

    Returns:
        Dict with per-metric deltas (with_memory - without_memory),
        plus the absolute scores for both conditions.
    """
    quality_with = evaluate_report_quality(reports_with_memory, ground_truths)
    quality_without = evaluate_report_quality(reports_without_memory, ground_truths)

    # Compute deltas
    delta_keys = [
        "avg_total_score",
        "root_cause_accuracy",
        "action_appropriateness",
        "severity_reasonableness",
        "avg_evidence_quality",
        "confidence_calibration",
    ]

    deltas = {
        f"delta_{k}": quality_with.get(k, 0.0) - quality_without.get(k, 0.0)
        for k in delta_keys
    }

    return {
        "with_memory": {k: quality_with[k] for k in delta_keys},
        "without_memory": {k: quality_without[k] for k in delta_keys},
        "deltas": deltas,
        "memory_helps": deltas["delta_avg_total_score"] > 0,
        "num_cases": len(ground_truths),
    }
