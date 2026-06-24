"""
Sentinel — Evaluation: Detection Metrics (§15).

Computes precision, recall, and F1 for the anomaly detection engine
against ground-truth fault injections.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple


@dataclass
class DetectionResult:
    """Result of a single detection run against ground truth."""
    run_id: str
    injected_fault_type: str       # ground truth
    injected_target: str           # column / job affected
    detected_anomalies: List[str]  # list of (metric) names detected
    was_detected: bool             # at least one anomaly matched the fault


@dataclass
class DetectionMetrics:
    """Aggregate detection performance metrics."""
    true_positives: int = 0
    false_positives: int = 0
    false_negatives: int = 0
    true_negatives: int = 0

    @property
    def precision(self) -> float:
        denom = self.true_positives + self.false_positives
        return self.true_positives / denom if denom > 0 else 0.0

    @property
    def recall(self) -> float:
        denom = self.true_positives + self.false_negatives
        return self.true_positives / denom if denom > 0 else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) > 0 else 0.0


def evaluate_detection(
    results: List[DetectionResult],
    clean_runs_detected: List[List[str]],
) -> DetectionMetrics:
    """Compute detection metrics from experimental results.

    Args:
        results: Detection results from fault-injected runs.
        clean_runs_detected: Anomalies detected on clean (no-fault) runs
            for computing false-positive rate.

    Returns:
        DetectionMetrics with precision/recall/F1.
    """
    metrics = DetectionMetrics()

    # Fault-injected runs
    for r in results:
        if r.was_detected:
            metrics.true_positives += 1
        else:
            metrics.false_negatives += 1

    # Clean runs (no faults) — any detection is a false positive
    for detected in clean_runs_detected:
        if detected:
            metrics.false_positives += len(detected)
        else:
            metrics.true_negatives += 1

    return metrics


def compute_detection_latency(
    results: List[Tuple[str, int]],
) -> Dict[str, float]:
    """Compute detection latency: runs between fault and escalation.

    Args:
        results: List of (fault_type, runs_to_escalation).

    Returns:
        Dict mapping fault_type → average latency in runs.
    """
    from collections import defaultdict
    latencies: Dict[str, List[int]] = defaultdict(list)

    for fault_type, runs in results:
        latencies[fault_type].append(runs)

    return {
        ft: sum(v) / len(v) if v else 0.0
        for ft, v in latencies.items()
    }


def compute_fp_trend(
    fp_counts_per_run: List[int],
) -> List[float]:
    """Compute rolling FP rate over successive runs.

    As suppression rules accumulate, FP rate should trend downward.

    Args:
        fp_counts_per_run: Number of false positives per clean run.

    Returns:
        Rolling average FP count (window=5).
    """
    if not fp_counts_per_run:
        return []

    window = min(5, len(fp_counts_per_run))
    result = []
    for i in range(len(fp_counts_per_run)):
        start = max(0, i - window + 1)
        chunk = fp_counts_per_run[start:i + 1]
        result.append(sum(chunk) / len(chunk))

    return result
