"""
Sentinel — Synthetic PaySim Data Generator (Section 16).

Generates batches of synthetic mobile-money transactions modelled after the
PaySim dataset.  Each batch represents one "day" of transactions with the
following columns:

    step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
    nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud

The generator is deterministic given a ``day`` seed, enabling reproducible
evaluation runs.

Design notes
------------
* ``step`` maps each day to 24 hourly slots (``day*24`` … ``day*24+23``).
* ``type`` follows realistic PaySim category weights.
* ``amount`` is drawn from a log-normal distribution (μ ≈ 50 000).
* Balance pairs are plausible: ``newbalanceOrig ≈ oldbalanceOrg − amount``
  for non-fraud rows, with noise.
* Fraud rate ~1 %; ``isFlaggedFraud`` is a very-rare subset of fraud rows.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────

TRANSACTION_TYPES: list[str] = [
    "PAYMENT",
    "TRANSFER",
    "CASH_OUT",
    "DEBIT",
    "CASH_IN",
]

# Weights approximate the real PaySim distribution:
# PAYMENT ~34%, TRANSFER ~8%, CASH_OUT ~35%, DEBIT ~1%, CASH_IN ~22%
TYPE_WEIGHTS: list[float] = [0.34, 0.08, 0.35, 0.01, 0.22]

# Log-normal parameters yielding mean ≈ 50 000
_LN_MU: float = 10.0       # ln-space mean  (e^10 ≈ 22 026)
_LN_SIGMA: float = 1.5     # ln-space std

FRAUD_RATE: float = 0.01          # ~1 % of transactions are fraudulent
FLAGGED_FRAUD_RATE: float = 0.05  # ~5 % of fraud rows also flagged


def _generate_ids(rng: np.random.Generator, n: int, prefix: str) -> np.ndarray:
    """Return *n* random IDs like ``C1234567`` or ``M1234567``."""
    nums = rng.integers(1_000_000, 9_999_999, size=n)
    return np.array([f"{prefix}{x}" for x in nums])


def generate_batch(day: int, num_rows: int = 10_000) -> pd.DataFrame:
    """Generate a single batch of synthetic PaySim-like transactions.

    Parameters
    ----------
    day : int
        Logical day index.  Determines the ``step`` range (``day*24`` …
        ``day*24+23``) and the random seed (for reproducibility).
    num_rows : int, optional
        Number of rows to generate (default ``10 000``).

    Returns
    -------
    pd.DataFrame
        DataFrame with all PaySim columns, ready for downstream transform
        stages.
    """
    rng = np.random.default_rng(seed=day)

    # ── step: hourly slots within the day ───────────────────────────────
    step_start = day * 24
    step = rng.integers(step_start, step_start + 24, size=num_rows)

    # ── type ────────────────────────────────────────────────────────────
    txn_type = rng.choice(TRANSACTION_TYPES, size=num_rows, p=TYPE_WEIGHTS)

    # ── amount (log-normal, mean ≈ 50k) ────────────────────────────────
    amount = rng.lognormal(mean=_LN_MU, sigma=_LN_SIGMA, size=num_rows)
    amount = np.round(amount, 2)

    # ── customer / merchant IDs ────────────────────────────────────────
    name_orig = _generate_ids(rng, num_rows, "C")
    name_dest = _generate_ids(rng, num_rows, "M")

    # ── balances — origin ───────────────────────────────────────────────
    # Old balance is drawn from a log-normal as well; new = old − amount
    old_balance_org = rng.lognormal(mean=11.0, sigma=1.5, size=num_rows)
    old_balance_org = np.round(np.maximum(old_balance_org, amount), 2)
    # Small noise so it's not perfectly deterministic
    noise_org = rng.normal(loc=0, scale=50, size=num_rows)
    new_balance_orig = np.round(
        np.maximum(old_balance_org - amount + noise_org, 0.0), 2
    )

    # ── balances — destination ──────────────────────────────────────────
    old_balance_dest = rng.lognormal(mean=11.0, sigma=1.5, size=num_rows)
    old_balance_dest = np.round(old_balance_dest, 2)
    noise_dest = rng.normal(loc=0, scale=50, size=num_rows)
    new_balance_dest = np.round(
        np.maximum(old_balance_dest + amount + noise_dest, 0.0), 2
    )

    # ── fraud labels ───────────────────────────────────────────────────
    is_fraud = (rng.random(num_rows) < FRAUD_RATE).astype(int)
    # isFlaggedFraud is a very-rare subset of fraud
    is_flagged_fraud = (
        is_fraud & (rng.random(num_rows) < FLAGGED_FRAUD_RATE)
    ).astype(int)

    # For fraud rows, scramble balances to simulate anomalous transfers
    fraud_mask = is_fraud.astype(bool)
    if fraud_mask.any():
        # Fraud: drain the origin account entirely
        new_balance_orig[fraud_mask] = 0.0
        # Fraud: destination gets a disproportionately large boost
        new_balance_dest[fraud_mask] = np.round(
            old_balance_dest[fraud_mask] + amount[fraud_mask], 2
        )

    # ── assemble DataFrame ─────────────────────────────────────────────
    df = pd.DataFrame(
        {
            "step": step,
            "type": txn_type,
            "amount": amount,
            "nameOrig": name_orig,
            "oldbalanceOrg": old_balance_org,
            "newbalanceOrig": new_balance_orig,
            "nameDest": name_dest,
            "oldbalanceDest": old_balance_dest,
            "newbalanceDest": new_balance_dest,
            "isFraud": is_fraud,
            "isFlaggedFraud": is_flagged_fraud,
        }
    )

    logger.info(
        "Generated batch: day=%d, rows=%d, fraud_count=%d",
        day,
        len(df),
        int(is_fraud.sum()),
    )
    return df
