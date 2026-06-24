"""
Sentinel — Operational signal collection.

Captures job-level metadata — status, duration, retries, exit code, and
upstream dependency list — so the reasoning engine can distinguish
data-quality issues from infrastructure failures.
"""

from __future__ import annotations

from datetime import datetime
from typing import List

from schemas import JobStatus, OperationalSignals


def collect_signals(
    run_id: str,
    stage_name: str,
    status: str,
    started_at: datetime,
    ended_at: datetime,
    upstream_jobs: List[str],
    retries: int = 0,
    exit_code: int = 0,
) -> OperationalSignals:
    """Build an :class:`OperationalSignals` snapshot for a single job step.

    Parameters
    ----------
    run_id : str
        Pipeline run identifier.
    stage_name : str
        Human-readable name of the job / stage.
    status : str
        One of the :class:`~schemas.JobStatus` enum values (e.g.
        ``"success"``, ``"failed"``).
    started_at, ended_at : datetime
        Wall-clock timestamps for the step.
    upstream_jobs : list[str]
        Names of direct upstream dependencies.
    retries : int
        Number of retry attempts before the final status was reached.
    exit_code : int
        Process exit code (0 = OK).

    Returns
    -------
    OperationalSignals
    """
    duration_seconds: float | None = None
    if started_at and ended_at:
        duration_seconds = (ended_at - started_at).total_seconds()

    return OperationalSignals(
        run_id=run_id,
        job_name=stage_name,
        status=JobStatus(status),
        started_at=started_at,
        ended_at=ended_at,
        duration_seconds=duration_seconds,
        retries=retries,
        exit_code=exit_code,
        upstream_jobs=upstream_jobs,
    )
