"""
Sentinel — Incident Text Summarization for Embedding (Section 8).

Produces a flat text summary from an :class:`Incident` that combines
anomaly details, the LLM root-cause analysis, the outcome, and the
resolution decision.  This summary is what gets embedded in the
vector store for similarity-based retrieval.
"""

from __future__ import annotations

from typing import List

from schemas import Anomaly, Incident


def build_summary_text(incident: Incident) -> str:
    """Build a flat text summary of an incident for embedding.

    The text is deliberately prose-like (not JSON) so the embedding
    model can produce meaningful similarity scores.  It includes:

    1. **Anomaly details** — dataset, stage, metric, check_type,
       observed vs expected values, deviation magnitude.
    2. **LLM root-cause** (if available) — likely_root_cause,
       caused_by category, evidence list.
    3. **Outcome** (if available) — whether it resolved, whether
       the fix worked, resolution method.
    4. **Resolution decision** (if available) — decision type,
       reason code, any manual-fix notes.

    Parameters
    ----------
    incident:
        A fully or partially resolved :class:`Incident`.

    Returns
    -------
    str
        A single flat text string suitable for embedding.
    """
    parts: List[str] = []

    # ── 1. Anomaly details ─────────────────────────────────────────
    if incident.anomalies:
        for anomaly in incident.anomalies:
            parts.append(_summarise_anomaly(anomaly))
    else:
        parts.append(
            f"Incident on dataset={incident.dataset}, "
            f"stage={incident.stage}."
        )

    # ── 2. LLM root-cause ─────────────────────────────────────────
    report = incident.report
    if report is not None:
        parts.append(
            f"Root cause: {report.likely_root_cause}. "
            f"Caused by: {report.caused_by.value}. "
            f"Confidence: {report.confidence:.2f}."
        )
        if report.evidence:
            evidence_str = "; ".join(report.evidence)
            parts.append(f"Evidence: {evidence_str}.")
        action = report.suggested_action
        if action is not None:
            parts.append(
                f"Suggested action: {action.type.value} "
                f"targeting '{action.target}'. "
                f"Rationale: {action.rationale}."
            )

    # ── 3. Outcome ─────────────────────────────────────────────────
    outcome = incident.outcome
    if outcome is not None:
        resolved_str = "resolved" if outcome.resolved else "unresolved"
        fix_str = (
            f"fix_worked={outcome.fix_worked}"
            if outcome.fix_worked is not None
            else "fix outcome unknown"
        )
        parts.append(
            f"Outcome: {resolved_str}. {fix_str}. "
            f"Resolution method: {outcome.resolution_method.value}."
        )
        if outcome.time_to_resolution_minutes is not None:
            parts.append(
                f"Time to resolution: "
                f"{outcome.time_to_resolution_minutes:.1f} minutes."
            )

    # ── 4. Resolution decision ─────────────────────────────────────
    resolution = incident.resolution
    if resolution is not None:
        parts.append(
            f"Decision: {resolution.decision.value}. "
            f"Reason: {resolution.reason.value}."
        )
        if resolution.manual_fix_note:
            parts.append(f"Manual fix note: {resolution.manual_fix_note}.")

    return " ".join(parts)


def _summarise_anomaly(anomaly: Anomaly) -> str:
    """Produce a concise text summary of a single anomaly."""
    return (
        f"Anomaly detected on dataset={anomaly.dataset}, "
        f"stage={anomaly.stage}, metric={anomaly.metric}, "
        f"check_type={anomaly.check_type.value}. "
        f"Observed={anomaly.observed}, expected={anomaly.expected}, "
        f"deviation={anomaly.deviation:.4f}, "
        f"severity={anomaly.severity_hint.value}."
    )
