"""
Sentinel — Reasoning Context Assembly (Section 7.5 / 8).

Assembles all signals into a single `ReasoningContext` to be sent
to the LLM reasoning engine.  The context is the *only* input the
LLM sees, so it must be complete and self-contained.
"""

from __future__ import annotations

from typing import List

from schemas import (
    Anomaly,
    ColumnSchema,
    IntentConfig,
    MemoryRecord,
    OperationalSignals,
    ReasoningContext,
    RunMetrics,
)
from config import config


def assemble_context(
    anomalies: List[Anomaly],
    intent: IntentConfig,
    recent_metrics: List[RunMetrics],
    operational: List[OperationalSignals],
    schema_current: List[ColumnSchema],
    similar_incidents: List[MemoryRecord],
) -> ReasoningContext:
    """Build a :class:`ReasoningContext` from all available signals.

    Parameters
    ----------
    anomalies:
        One or more anomalies detected in a single run/group.  The
        **first** element is treated as the primary anomaly (the context
        model accepts a single ``Anomaly``).
    intent:
        The ``IntentConfig`` for the dataset under inspection.
    recent_metrics:
        Historical ``RunMetrics`` for the same dataset/stage (last *N*
        runs, controlled by ``config.BASELINE_WINDOW``).
    operational:
        ``OperationalSignals`` for this run **and** upstream jobs.
    schema_current:
        Column-level schema snapshot of the current run.
    similar_incidents:
        Top-k ``MemoryRecord`` objects retrieved from the vector store
        for contextual grounding.

    Returns
    -------
    ReasoningContext
        A fully-assembled context ready for prompt serialization.
    """
    primary_anomaly: Anomaly = anomalies[0]

    return ReasoningContext(
        anomaly=primary_anomaly,
        intent=intent,
        recent_metrics=recent_metrics,
        operational=operational,
        schema_current=schema_current,
        code_version=config.code_version(),
        similar_incidents=similar_incidents,
    )
