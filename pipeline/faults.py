"""
Sentinel — Fault Injection Harness (Section 8 / 15).

Injects labelled faults into clean batches for evaluation.  Each fault
produces a corrupted batch **and** a ground-truth label that the detection
and reasoning layers are measured against.

Supported fault types
---------------------
* ``row_drop``           — randomly drop a percentage of rows.
* ``column_null``        — set a column to ``NULL`` for a percentage of rows.
* ``schema_change``      — drop or rename a column.
* ``distribution_shift`` — multiply a numeric column by a constant factor.
* ``stale_data``         — set the ``step`` column to old (stale) values.
* ``operational_cause``  — mark an upstream job as failed/skipped **and**
                           inject a downstream data fault to simulate the
                           causal chain (e.g. failed ingest → missing rows).

The ground-truth label dict conforms to the spec contract::

    {
        "fault_type": str,
        "target": str,           # column name or job name
        "params": dict,          # fault-specific parameters
        "caused_by": str         # one of CausedBy enum values
    }
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd

from schemas import CausedBy, JobStatus, OperationalSignals

logger = logging.getLogger(__name__)


# ── FaultSpec dataclass ────────────────────────────────────────────────────

@dataclass
class FaultSpec:
    """Declarative specification for a single fault injection.

    Attributes
    ----------
    fault_type : str
        One of ``row_drop``, ``column_null``, ``schema_change``,
        ``distribution_shift``, ``stale_data``, ``operational_cause``.
    target : str
        Column name affected (or upstream job name for operational faults).
    params : dict
        Fault-specific parameters.  Expected keys per type:

        * ``row_drop``:           ``{"drop_pct": float}``  (0.0–1.0)
        * ``column_null``:        ``{"null_pct": float}``  (0.0–1.0)
        * ``schema_change``:      ``{"action": "drop"|"rename", "new_name": str|None}``
        * ``distribution_shift``: ``{"factor": float}``
        * ``stale_data``:         ``{"stale_days": int}``
        * ``operational_cause``:  ``{"job_status": "failed"|"skipped",
                                     "downstream_fault_type": str,
                                     "downstream_target": str,
                                     "downstream_params": dict}``
    seed : int | None
        Random seed for reproducibility.
    """

    fault_type: str
    target: str
    params: Dict[str, Any] = field(default_factory=dict)
    seed: Optional[int] = None


# ── Individual fault injectors ─────────────────────────────────────────────

def _inject_row_drop(
    df: pd.DataFrame, spec: FaultSpec, rng: np.random.Generator
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Drop a random percentage of rows."""
    drop_pct = spec.params.get("drop_pct", 0.5)
    n_drop = int(len(df) * drop_pct)
    drop_indices = rng.choice(df.index, size=n_drop, replace=False)
    corrupted = df.drop(index=drop_indices).reset_index(drop=True)

    label = {
        "fault_type": "row_drop",
        "target": "rows",
        "params": {"drop_pct": drop_pct, "rows_dropped": n_drop},
        "caused_by": CausedBy.data_source.value,
    }
    logger.info("Injected row_drop: dropped %d / %d rows", n_drop, len(df))
    return corrupted, label


def _inject_column_null(
    df: pd.DataFrame, spec: FaultSpec, rng: np.random.Generator
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Set a column to NULL for a percentage of rows."""
    null_pct = spec.params.get("null_pct", 0.5)
    target_col = spec.target

    if target_col not in df.columns:
        raise ValueError(f"Column '{target_col}' not found in DataFrame")

    corrupted = df.copy()
    n_null = int(len(corrupted) * null_pct)
    null_indices = rng.choice(corrupted.index, size=n_null, replace=False)
    corrupted.loc[null_indices, target_col] = np.nan

    label = {
        "fault_type": "column_null",
        "target": target_col,
        "params": {"null_pct": null_pct, "rows_nulled": n_null},
        "caused_by": CausedBy.data_source.value,
    }
    logger.info(
        "Injected column_null: set %d rows of '%s' to NULL", n_null, target_col
    )
    return corrupted, label


def _inject_schema_change(
    df: pd.DataFrame, spec: FaultSpec, rng: np.random.Generator
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Drop or rename a column to simulate a schema change."""
    action = spec.params.get("action", "drop")
    target_col = spec.target

    if target_col not in df.columns:
        raise ValueError(f"Column '{target_col}' not found in DataFrame")

    corrupted = df.copy()

    if action == "rename":
        new_name = spec.params.get("new_name", f"{target_col}_v2")
        corrupted = corrupted.rename(columns={target_col: new_name})
        detail = {"action": "rename", "new_name": new_name}
    else:  # default: drop
        corrupted = corrupted.drop(columns=[target_col])
        detail = {"action": "drop"}

    label = {
        "fault_type": "schema_change",
        "target": target_col,
        "params": detail,
        "caused_by": CausedBy.schema_change.value,
    }
    logger.info("Injected schema_change: %s column '%s'", action, target_col)
    return corrupted, label


def _inject_distribution_shift(
    df: pd.DataFrame, spec: FaultSpec, rng: np.random.Generator
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Multiply a numeric column by a constant factor."""
    factor = spec.params.get("factor", 10.0)
    target_col = spec.target

    if target_col not in df.columns:
        raise ValueError(f"Column '{target_col}' not found in DataFrame")

    corrupted = df.copy()
    corrupted[target_col] = corrupted[target_col] * factor

    label = {
        "fault_type": "distribution_shift",
        "target": target_col,
        "params": {"factor": factor},
        "caused_by": CausedBy.data_source.value,
    }
    logger.info(
        "Injected distribution_shift: multiplied '%s' by %.1f", target_col, factor
    )
    return corrupted, label


def _inject_stale_data(
    df: pd.DataFrame, spec: FaultSpec, rng: np.random.Generator
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Set the step column to old values, simulating stale/delayed data."""
    stale_days = spec.params.get("stale_days", 7)

    corrupted = df.copy()
    # Move the step values back by stale_days * 24 hours
    stale_offset = stale_days * 24
    corrupted["step"] = corrupted["step"] - stale_offset

    label = {
        "fault_type": "stale_data",
        "target": "step",
        "params": {"stale_days": stale_days, "step_offset": stale_offset},
        "caused_by": CausedBy.pipeline_logic.value,
    }
    logger.info(
        "Injected stale_data: shifted step back by %d hours (%d days)",
        stale_offset,
        stale_days,
    )
    return corrupted, label


def _inject_operational_cause(
    df: pd.DataFrame, spec: FaultSpec, rng: np.random.Generator
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Simulate an upstream job failure that causes a downstream data fault.

    Returns the corrupted batch with an embedded ``OperationalSignals``
    object stored in the label under ``"operational_signal"``.
    """
    job_status_str = spec.params.get("job_status", "failed")
    job_status = JobStatus(job_status_str)

    downstream_fault_type = spec.params.get("downstream_fault_type", "row_drop")
    downstream_target = spec.params.get("downstream_target", "rows")
    downstream_params = spec.params.get("downstream_params", {"drop_pct": 0.5})

    # Create the operational signal for the upstream job
    now = datetime.utcnow()
    op_signal = OperationalSignals(
        run_id=str(uuid.uuid4())[:8],
        job_name=spec.target,
        status=job_status,
        started_at=now - timedelta(minutes=5),
        ended_at=now,
        duration_seconds=300.0,
        retries=2 if job_status == JobStatus.failed else 0,
        exit_code=1 if job_status == JobStatus.failed else None,
        upstream_jobs=[],
    )

    # Also inject the downstream data fault
    downstream_spec = FaultSpec(
        fault_type=downstream_fault_type,
        target=downstream_target,
        params=downstream_params,
        seed=spec.seed,
    )
    corrupted, downstream_label = inject_fault(df, downstream_spec)

    label = {
        "fault_type": "operational_cause",
        "target": spec.target,
        "params": {
            "job_status": job_status_str,
            "downstream_fault": downstream_label,
        },
        "caused_by": CausedBy.upstream_job.value,
        "operational_signal": op_signal,
    }
    logger.info(
        "Injected operational_cause: upstream '%s' %s → downstream %s",
        spec.target,
        job_status_str,
        downstream_fault_type,
    )
    return corrupted, label


# ── Dispatcher table ───────────────────────────────────────────────────────

_INJECTORS = {
    "row_drop": _inject_row_drop,
    "column_null": _inject_column_null,
    "schema_change": _inject_schema_change,
    "distribution_shift": _inject_distribution_shift,
    "stale_data": _inject_stale_data,
    "operational_cause": _inject_operational_cause,
}


# ── Public API ─────────────────────────────────────────────────────────────

def inject_fault(
    batch: pd.DataFrame, spec: FaultSpec
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """Inject a fault into a clean batch and return ground-truth labels.

    Parameters
    ----------
    batch : pd.DataFrame
        Clean batch to corrupt.
    spec : FaultSpec
        Fault specification describing the type and parameters.

    Returns
    -------
    Tuple[pd.DataFrame, Dict[str, Any]]
        ``(corrupted_batch, ground_truth_label)`` where the label dict
        contains ``{fault_type, target, params, caused_by}`` conforming
        to the spec §8 contract.

    Raises
    ------
    ValueError
        If ``spec.fault_type`` is not a recognised fault type.
    """
    injector = _INJECTORS.get(spec.fault_type)
    if injector is None:
        raise ValueError(
            f"Unknown fault type '{spec.fault_type}'. "
            f"Supported: {sorted(_INJECTORS.keys())}"
        )

    rng = np.random.default_rng(seed=spec.seed)
    corrupted, label = injector(batch, spec, rng)

    logger.info(
        "Fault injection complete: type=%s, original_rows=%d, result_rows=%d",
        spec.fault_type,
        len(batch),
        len(corrupted),
    )
    return corrupted, label
