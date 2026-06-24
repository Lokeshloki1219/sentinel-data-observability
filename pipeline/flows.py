"""
Sentinel — Pipeline Orchestration (Sections 10, 16).

Runs the end-to-end PaySim pipeline for a single "day":

    ingest → (optional fault injection) → raw → cleaned → enriched → fraud_features

Each stage is timed and emits an :class:`~schemas.OperationalSignals` object.
The complete run manifest is returned as a dict for downstream observability.

Stage names (per spec §16)::

    raw_transactions → cleaned_typed → enriched → fraud_scoring_features
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from schemas import JobStatus, OperationalSignals
from config import config


def reference_now_for_day(day: int) -> datetime:
    """Synthetic wall-clock for a run: the end of the logical *day*.

    A fresh batch's latest ``step`` is ``day*24 + 23``; anchoring "now" one
    hour later (``day*24 + 24``) yields ~60 min freshness for healthy data and
    large freshness once the ``stale_data`` fault shifts ``step`` backwards.
    """
    return config.STEP_EPOCH + timedelta(hours=day * 24 + 24)

from pipeline.ingest import generate_batch
from pipeline.faults import FaultSpec, inject_fault
from pipeline.transform.transform import (
    stage_raw,
    stage_cleaned,
    stage_enriched,
    stage_fraud_features,
)

logger = logging.getLogger(__name__)

# ── Stage metadata ─────────────────────────────────────────────────────────

# Ordered pipeline stages, each with (stage_name, callable, upstream_jobs)
_STAGES: list[tuple[str, Any, list[str]]] = [
    ("raw_transactions", stage_raw, []),
    ("cleaned_typed", stage_cleaned, ["raw_transactions"]),
    ("enriched", stage_enriched, ["cleaned_typed"]),
    ("fraud_scoring_features", stage_fraud_features, ["enriched"]),
]


def _make_op_signal(
    run_id: str,
    job_name: str,
    status: JobStatus,
    started_at: datetime,
    ended_at: datetime,
    upstream_jobs: List[str],
    retries: int = 0,
    exit_code: Optional[int] = None,
) -> OperationalSignals:
    """Create an OperationalSignals for a completed stage.

    Parameters
    ----------
    run_id : str
        Shared run identifier for all stages in this pipeline execution.
    job_name : str
        Stage / job name matching the spec §16 naming convention.
    status : JobStatus
        Final status of the job.
    started_at, ended_at : datetime
        Wall-clock timestamps.
    upstream_jobs : list[str]
        Declared upstream dependencies (lineage).
    retries : int
        Number of retry attempts.
    exit_code : int | None
        Process exit code (``0`` for success, non-zero for failure).
    """
    duration = (ended_at - started_at).total_seconds()
    return OperationalSignals(
        run_id=run_id,
        job_name=job_name,
        status=status,
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=round(duration, 3),
        retries=retries,
        exit_code=exit_code,
        upstream_jobs=upstream_jobs,
    )


def run_pipeline(
    day: int,
    fault_spec: Optional[FaultSpec] = None,
    num_rows: int = 10_000,
) -> Dict[str, Any]:
    """Execute the full PaySim pipeline for a single day.

    Parameters
    ----------
    day : int
        Logical day index passed to :func:`~pipeline.ingest.generate_batch`.
    fault_spec : FaultSpec | None, optional
        If provided, faults are injected into the raw batch *before* the
        transform stages run.  The ground-truth label is included in the
        returned manifest.
    num_rows : int, optional
        Number of rows to generate (default ``10 000``).

    Returns
    -------
    dict
        Run manifest with the following keys:

        * ``run_id`` — unique run identifier.
        * ``day`` — input day index.
        * ``batches`` — ``Dict[str, pd.DataFrame]`` per stage name.
        * ``operational_signals`` — ``Dict[str, OperationalSignals]`` per stage.
        * ``fault_injected`` — ``bool`` indicating whether faults were applied.
        * ``fault_label`` — ground-truth label dict (or ``None``).
        * ``fault_operational_signal`` — ``OperationalSignals`` from an
          operational-cause fault (or ``None``).
    """
    run_id = str(uuid.uuid4())[:12]
    logger.info("Starting pipeline run %s for day %d", run_id, day)

    # ── 1. Ingest ──────────────────────────────────────────────────────
    batch = generate_batch(day=day, num_rows=num_rows)

    # ── 2. Fault injection (optional) ──────────────────────────────────
    fault_label: Optional[Dict[str, Any]] = None
    fault_op_signal: Optional[OperationalSignals] = None

    if fault_spec is not None:
        batch, fault_label = inject_fault(batch, fault_spec)
        # Extract operational signal if this was an operational-cause fault
        if (
            fault_label
            and fault_label.get("fault_type") == "operational_cause"
            and "operational_signal" in fault_label
        ):
            fault_op_signal = fault_label.pop("operational_signal")
        logger.info("Fault injected: %s", fault_label)

    # ── 3. Transform stages ────────────────────────────────────────────
    batches: Dict[str, pd.DataFrame] = {}
    op_signals: Dict[str, OperationalSignals] = {}
    current_batch = batch

    # Base time for realistic stage timing
    pipeline_start = datetime.utcnow()
    stage_clock = pipeline_start

    for stage_name, stage_fn, upstream_jobs in _STAGES:
        started_at = stage_clock

        try:
            current_batch = stage_fn(current_batch)
            status = JobStatus.success
            exit_code = 0
        except Exception as exc:
            logger.error(
                "Stage '%s' failed in run %s: %s", stage_name, run_id, exc
            )
            status = JobStatus.failed
            exit_code = 1
            # On failure, keep whatever batch we had from the previous stage
            current_batch = current_batch

        # Simulate realistic stage duration (1–5 seconds spread)
        stage_duration = timedelta(
            seconds=1.0 + hash(stage_name) % 4  # deterministic but varied
        )
        ended_at = started_at + stage_duration
        stage_clock = ended_at + timedelta(milliseconds=100)  # small gap

        batches[stage_name] = current_batch.copy()
        op_signals[stage_name] = _make_op_signal(
            run_id=run_id,
            job_name=stage_name,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
            upstream_jobs=upstream_jobs,
            exit_code=exit_code,
        )

    # ── 4. Assemble manifest ───────────────────────────────────────────
    manifest: Dict[str, Any] = {
        "run_id": run_id,
        "day": day,
        "reference_now": reference_now_for_day(day),
        "batches": batches,
        "operational_signals": op_signals,
        "fault_injected": fault_spec is not None,
        "fault_label": fault_label,
        "fault_operational_signal": fault_op_signal,
    }

    logger.info(
        "Pipeline run %s complete: %d stages, fault_injected=%s",
        run_id,
        len(batches),
        fault_spec is not None,
    )
    return manifest
