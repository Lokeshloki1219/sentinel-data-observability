"""
Sentinel — Statistical Detection Functions.

Pure-function statistical primitives used by the detection rule engine.
Each function is deterministic given its inputs and carries no side effects.
"""

from __future__ import annotations

import logging
import math
from typing import Dict, List, Tuple

import numpy as np
from scipy import stats as sp_stats

logger = logging.getLogger(__name__)

# Small constant to avoid log(0) / division-by-zero in PSI calculation.
_EPS = 1e-10

# Sentinel z-score for a maximal (degenerate) deviation: history has zero
# variance but the current value differs.  A large *finite* value keeps the
# deviation JSON-serialisable (unlike ``inf``) while guaranteeing escalation.
_DEGENERATE_Z = 100.0


def compute_zscore(current: float, history: List[float]) -> float:
    """Compute a rolling z-score of *current* against *history*.

    Parameters
    ----------
    current : float
        The latest observed value.
    history : list[float]
        Past observed values (at least 2 are needed for a meaningful
        standard deviation).

    Returns
    -------
    float
        The z-score.  Returns ``0.0`` when *history* is too short.  When the
        history has zero variance, returns ``0.0`` if *current* equals the
        constant baseline, otherwise a large signed sentinel (a constant
        series that suddenly changes is maximally anomalous).
    """
    if len(history) < 2:
        return 0.0
    arr = np.array(history, dtype=np.float64)
    mu = float(np.mean(arr))
    sigma = float(np.std(arr, ddof=1))
    if sigma == 0.0:
        if math.isclose(current, mu):
            return 0.0
        return math.copysign(_DEGENERATE_Z, current - mu)
    return (current - mu) / sigma


def compute_psi(
    current_dist: Dict[str, float],
    baseline_dist: Dict[str, float],
    bins: int = 10,
) -> float:
    """Compute the Population Stability Index (PSI) between two
    categorical distributions.

    Both *current_dist* and *baseline_dist* map category names to
    relative frequencies (should each sum to ≈ 1.0).

    PSI = Σ (p_i − q_i) × ln(p_i / q_i)

    Parameters
    ----------
    current_dist : dict[str, float]
        Current (observed) distribution.
    baseline_dist : dict[str, float]
        Expected / baseline distribution.
    bins : int
        Unused for categorical PSI but kept in the signature for API
        symmetry with histogram-based PSI.

    Returns
    -------
    float
        PSI value.  Typical thresholds:
        - < 0.10  → no significant shift
        - 0.10–0.25 → moderate shift
        - > 0.25  → significant shift
    """
    all_keys = set(current_dist.keys()) | set(baseline_dist.keys())
    if not all_keys:
        return 0.0

    psi = 0.0
    for key in all_keys:
        p = current_dist.get(key, 0.0) + _EPS  # current
        q = baseline_dist.get(key, 0.0) + _EPS  # baseline
        psi += (p - q) * np.log(p / q)

    return float(psi)


def compute_ks_test(
    current_values: List[float],
    baseline_values: List[float],
) -> Tuple[float, float]:
    """Run a two-sample Kolmogorov–Smirnov test.

    Parameters
    ----------
    current_values : list[float]
        Observed sample.
    baseline_values : list[float]
        Reference / baseline sample.

    Returns
    -------
    tuple[float, float]
        ``(ks_statistic, p_value)``.  A small *p_value* (< 0.05) suggests
        the two samples come from different distributions.
    """
    if len(current_values) < 2 or len(baseline_values) < 2:
        return 0.0, 1.0

    stat, p_value = sp_stats.ks_2samp(current_values, baseline_values)
    return float(stat), float(p_value)
