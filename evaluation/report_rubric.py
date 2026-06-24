"""
Sentinel — Evaluation: Report Quality Rubric (§15).

Scores LLM ReasoningOutput quality using a structured rubric.
Optionally uses an LLM-as-judge for automated evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class RubricScore:
    """Score for a single report against the rubric."""
    incident_id: str
    root_cause_correct: bool    # Was the root cause identification correct?
    action_appropriate: bool    # Was the suggested action appropriate?
    severity_reasonable: bool   # Was the severity assignment reasonable?
    evidence_quality: float     # 0..1 — are the evidence items relevant?
    confidence_calibrated: bool # Is confidence appropriate for the diagnosis?
    total_score: float = 0.0    # Computed weighted total

    def __post_init__(self):
        self.total_score = (
            0.30 * float(self.root_cause_correct)
            + 0.25 * float(self.action_appropriate)
            + 0.20 * float(self.severity_reasonable)
            + 0.15 * self.evidence_quality
            + 0.10 * float(self.confidence_calibrated)
        )


def evaluate_report_quality(
    reports: List[dict],
    ground_truths: List[dict],
) -> Dict[str, float]:
    """Evaluate report quality against ground truth.

    Args:
        reports: List of ReasoningOutput dicts.
        ground_truths: List of fault injection labels with expected
            {fault_type, target, caused_by}.

    Returns:
        Dict with aggregate quality metrics.
    """
    scores: List[RubricScore] = []

    for report, gt in zip(reports, ground_truths):
        if report is None:
            scores.append(RubricScore(
                incident_id=gt.get("run_id", ""),
                root_cause_correct=False,
                action_appropriate=False,
                severity_reasonable=False,
                evidence_quality=0.0,
                confidence_calibrated=False,
            ))
            continue

        # Root cause: check if caused_by matches
        root_cause_correct = report.get("caused_by", "") == gt.get("caused_by", "")

        # Action: check if suggested action type is reasonable for the fault
        action_type = report.get("suggested_action", {}).get("type", "none")
        fault_type = gt.get("fault_type", "")
        action_appropriate = _is_action_appropriate(action_type, fault_type)

        # Severity: check if reasonable given the fault
        severity = report.get("severity", "low")
        severity_reasonable = severity in ("high", "critical") or fault_type in ("stale_data",)

        # Evidence: check if at least some evidence items are present
        evidence = report.get("evidence", [])
        evidence_quality = min(len(evidence) / 3, 1.0) if evidence else 0.0

        # Confidence calibration: high confidence with correct cause = good
        confidence = report.get("confidence", 0.0)
        confidence_calibrated = (
            (root_cause_correct and confidence >= 0.6)
            or (not root_cause_correct and confidence < 0.5)
        )

        scores.append(RubricScore(
            incident_id=gt.get("run_id", ""),
            root_cause_correct=root_cause_correct,
            action_appropriate=action_appropriate,
            severity_reasonable=severity_reasonable,
            evidence_quality=evidence_quality,
            confidence_calibrated=confidence_calibrated,
        ))

    n = len(scores) or 1
    return {
        "avg_total_score": sum(s.total_score for s in scores) / n,
        "root_cause_accuracy": sum(s.root_cause_correct for s in scores) / n,
        "action_appropriateness": sum(s.action_appropriate for s in scores) / n,
        "severity_reasonableness": sum(s.severity_reasonable for s in scores) / n,
        "avg_evidence_quality": sum(s.evidence_quality for s in scores) / n,
        "confidence_calibration": sum(s.confidence_calibrated for s in scores) / n,
        "num_reports": len(scores),
        "scores": scores,
    }


def _is_action_appropriate(action_type: str, fault_type: str) -> bool:
    """Heuristic: is the suggested action reasonable for the fault type?"""
    reasonable_mappings = {
        "row_drop": {"rerun_job", "quarantine_batch", "backfill"},
        "column_null": {"rerun_job", "quarantine_batch"},
        "schema_change": {"rerun_job", "manual", "none"},
        "distribution_shift": {"quarantine_batch", "manual", "none"},
        "stale_data": {"rerun_job", "backfill"},
        "operational_cause": {"rerun_job"},
    }
    acceptable = reasonable_mappings.get(fault_type, {"manual", "none"})
    return action_type in acceptable
