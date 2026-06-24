"""
Sentinel — RunMetrics computation.

Derives :class:`~schemas.RunMetrics` from a raw ``pandas.DataFrame`` batch,
computing row counts, freshness, schema fingerprints, null rates, numeric
distribution summaries, and categorical frequency tables.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from config import config
from schemas import ColumnSchema, NumericStats, RunMetrics

logger = logging.getLogger(__name__)


def compute_metrics(
    run_id: str,
    dataset: str,
    stage: str,
    batch: pd.DataFrame,
    key_columns: List[str],
    reference_now: Optional[datetime] = None,
) -> RunMetrics:
    """Compute a full :class:`RunMetrics` snapshot for *batch*.

    Parameters
    ----------
    run_id : str
        Unique identifier for this pipeline run.
    dataset : str
        Logical dataset name (must match ``IntentConfig.dataset``).
    stage : str
        Pipeline stage / step that produced *batch*.
    batch : pd.DataFrame
        The data to profile.
    key_columns : list[str]
        Columns to include in null-rate, numeric-stats, and categorical-dist
        profiling.  Typically sourced from ``IntentConfig.key_columns``.
    reference_now : datetime, optional
        The "now" against which freshness is measured.  The pipeline passes a
        synthetic clock derived from the batch's ``step`` axis so freshness is
        deterministic and reproducible.  Defaults to wall-clock UTC.

    Returns
    -------
    RunMetrics
        Fully populated metrics snapshot.
    """
    now = reference_now or datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    # ── Row count ──────────────────────────────────────────────────────
    row_count: int = len(batch)

    # ── Event-time freshness ───────────────────────────────────────────
    event_time_max = _compute_event_time_max(batch, now)
    freshness_minutes = (now - event_time_max).total_seconds() / 60.0

    # ── Schema fingerprint ─────────────────────────────────────────────
    schema_cols: List[ColumnSchema] = [
        ColumnSchema(name=col, dtype=str(dtype))
        for col, dtype in batch.dtypes.items()
    ]
    schema_hash = RunMetrics.compute_schema_hash(schema_cols)

    # ── Null rates for key columns ─────────────────────────────────────
    null_rate: Dict[str, float] = {}
    for col in key_columns:
        if col in batch.columns:
            null_rate[col] = float(batch[col].isna().mean())

    # ── Numeric stats for numeric key columns ──────────────────────────
    numeric_stats: Dict[str, NumericStats] = {}
    for col in key_columns:
        if col in batch.columns and pd.api.types.is_numeric_dtype(batch[col]):
            series = batch[col].dropna()
            if len(series) == 0:
                continue
            numeric_stats[col] = NumericStats(
                mean=float(series.mean()),
                std=float(series.std(ddof=0)),
                p05=float(np.percentile(series, 5)),
                p50=float(np.percentile(series, 50)),
                p95=float(np.percentile(series, 95)),
                min=float(series.min()),
                max=float(series.max()),
            )

    # ── Categorical distributions for low-cardinality columns ──────────
    categorical_dist: Dict[str, Dict[str, float]] = {}
    for col in key_columns:
        if col not in batch.columns:
            continue
        if batch[col].dtype in ("object", "category") or pd.api.types.is_string_dtype(
            batch[col]
        ):
            nunique = batch[col].nunique(dropna=True)
            if nunique < 50:
                freq = batch[col].value_counts(normalize=True, dropna=True)
                categorical_dist[col] = {str(k): float(v) for k, v in freq.items()}

    return RunMetrics(
        run_id=run_id,
        dataset=dataset,
        stage=stage,
        ts_run=now,
        event_time_max=event_time_max,
        row_count=row_count,
        freshness_minutes=freshness_minutes,
        schema_hash=schema_hash,
        schema_=schema_cols,
        null_rate=null_rate,
        numeric_stats=numeric_stats,
        categorical_dist=categorical_dist,
    )


def _compute_event_time_max(batch: pd.DataFrame, fallback: datetime) -> datetime:
    """Determine the maximum business event-time in *batch*.

    Resolution order:

    1. **PaySim ``step`` axis** — ``step`` is an integer hourly index, so the
       latest business timestamp is ``STEP_EPOCH + max(step) hours``.  This is
       what makes freshness (and the ``stale_data`` fault, which shifts
       ``step`` backwards) observable.
    2. **Datetime columns** — any column whose name contains ``time``/``date``/
       ``ts``/``_at`` is parsed and its max taken.
    3. **Fallback** — *fallback* (the reference "now") when neither applies.
    """
    # 1. PaySim step axis (preferred for this pipeline).
    if "step" in batch.columns:
        steps = pd.to_numeric(batch["step"], errors="coerce").dropna()
        if not steps.empty:
            max_step = int(steps.max())
            return config.STEP_EPOCH + timedelta(hours=max_step)

    # 2. Explicit datetime columns.
    candidate_cols = [
        c
        for c in batch.columns
        if any(tok in c.lower() for tok in ("time", "date", "ts", "_at"))
    ]
    for col in candidate_cols:
        try:
            parsed = pd.to_datetime(batch[col], errors="coerce", utc=True)
            max_val = parsed.max()
            if pd.notna(max_val):
                return max_val.to_pydatetime()
        except Exception:
            continue

    # 3. Fallback.
    return fallback
