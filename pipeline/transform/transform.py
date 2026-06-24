"""
Sentinel — Transformation Pipeline (Section 16).

Implements the four-stage transformation for PaySim transactions:

    raw_transactions → cleaned_typed → enriched → fraud_scoring_features

Each stage accepts a ``pd.DataFrame``, applies its transformations, and
returns a new DataFrame.  Stages are designed to be composable and
independently testable.

Pipeline stages monitored (from spec §16):
    1. ``raw_transactions``       — pass-through with type validation
    2. ``cleaned_typed``          — cast types, drop invalid rows
    3. ``enriched``               — add balance deltas, account-level aggregates
    4. ``fraud_scoring_features`` — add fraud scoring features
"""

from __future__ import annotations

import logging
from typing import Final

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Expected Schema ────────────────────────────────────────────────────────

PAYSIM_COLUMNS: Final[list[str]] = [
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]

VALID_TYPES: Final[set[str]] = {
    "PAYMENT",
    "TRANSFER",
    "CASH_OUT",
    "DEBIT",
    "CASH_IN",
}


# ── Stage 1: raw_transactions ─────────────────────────────────────────────

def stage_raw(batch: pd.DataFrame) -> pd.DataFrame:
    """Stage 1 — Raw ingestion with schema validation.

    Validates that all expected PaySim columns are present and returns
    the batch as-is (pass-through).  This is the entry-point into the
    transform pipeline.

    Parameters
    ----------
    batch : pd.DataFrame
        Raw batch from :func:`pipeline.ingest.generate_batch`.

    Returns
    -------
    pd.DataFrame
        Same data, validated for column presence.

    Raises
    ------
    ValueError
        If required columns are missing.
    """
    missing = set(PAYSIM_COLUMNS) - set(batch.columns)
    if missing:
        raise ValueError(
            f"Raw batch is missing required columns: {sorted(missing)}"
        )

    logger.info(
        "stage_raw: validated %d rows, %d columns", len(batch), len(batch.columns)
    )
    return batch.copy()


# ── Stage 2: cleaned_typed ─────────────────────────────────────────────────

def stage_cleaned(raw: pd.DataFrame) -> pd.DataFrame:
    """Stage 2 — Type casting and invalid-row removal.

    * Casts numeric columns to ``float64`` and integer labels to ``int64``.
    * Drops rows where ``amount`` is negative or NaN.
    * Drops rows with unrecognised ``type`` values.
    * Resets the index after row removal.

    Parameters
    ----------
    raw : pd.DataFrame
        Output of :func:`stage_raw`.

    Returns
    -------
    pd.DataFrame
        Cleaned and consistently-typed DataFrame.
    """
    df = raw.copy()

    # ── cast numeric columns ────────────────────────────────────────────
    numeric_cols = [
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Cast integer columns
    int_cols = ["step", "isFraud", "isFlaggedFraud"]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    initial_rows = len(df)

    # ── drop invalid rows ──────────────────────────────────────────────
    # Remove rows with NaN in critical numeric columns
    df = df.dropna(subset=["amount", "step"])

    # Remove negative amounts
    df = df[df["amount"] >= 0]

    # Remove unrecognised transaction types
    df = df[df["type"].isin(VALID_TYPES)]

    # Ensure integer columns are int after NaN removal
    for col in int_cols:
        df[col] = df[col].astype("int64")

    df = df.reset_index(drop=True)

    dropped = initial_rows - len(df)
    logger.info(
        "stage_cleaned: %d → %d rows (dropped %d invalid)",
        initial_rows,
        len(df),
        dropped,
    )
    return df


# ── Stage 3: enriched ─────────────────────────────────────────────────────

def stage_enriched(cleaned: pd.DataFrame) -> pd.DataFrame:
    """Stage 3 — Feature enrichment: balance deltas and account aggregates.

    Adds computed columns:

    * ``balance_delta_org`` — ``newbalanceOrig − oldbalanceOrg``
    * ``balance_delta_dest`` — ``newbalanceDest − oldbalanceDest``
    * ``balance_error_org`` — discrepancy: ``oldbalanceOrg − amount − newbalanceOrig``
    * ``orig_txn_count`` — number of transactions per origin account
    * ``orig_total_amount`` — total transaction value per origin account
    * ``dest_txn_count`` — number of transactions per destination account

    Parameters
    ----------
    cleaned : pd.DataFrame
        Output of :func:`stage_cleaned`.

    Returns
    -------
    pd.DataFrame
        Enriched DataFrame with additional columns.
    """
    df = cleaned.copy()

    # ── balance deltas ─────────────────────────────────────────────────
    df["balance_delta_org"] = np.round(
        df["newbalanceOrig"] - df["oldbalanceOrg"], 2
    )
    df["balance_delta_dest"] = np.round(
        df["newbalanceDest"] - df["oldbalanceDest"], 2
    )

    # Discrepancy between expected and actual origin balance change
    # For a clean non-fraud row this should be close to zero
    df["balance_error_org"] = np.round(
        df["oldbalanceOrg"] - df["amount"] - df["newbalanceOrig"], 2
    )

    # ── account-level aggregates ───────────────────────────────────────
    orig_agg = (
        df.groupby("nameOrig")["amount"]
        .agg(orig_txn_count="count", orig_total_amount="sum")
        .reset_index()
    )
    df = df.merge(orig_agg, on="nameOrig", how="left")

    dest_agg = (
        df.groupby("nameDest")["amount"]
        .agg(dest_txn_count="count")
        .reset_index()
    )
    df = df.merge(dest_agg, on="nameDest", how="left")

    logger.info("stage_enriched: %d rows, %d columns", len(df), len(df.columns))
    return df


# ── Stage 4: fraud_scoring_features ───────────────────────────────────────

def stage_fraud_features(enriched: pd.DataFrame) -> pd.DataFrame:
    """Stage 4 — Fraud scoring features.

    Adds features useful for downstream fraud detection / monitoring:

    * ``amount_zscore`` — z-score of ``amount`` within the batch.
    * ``balance_ratio`` — ``amount / (oldbalanceOrg + 1)`` — how much of the
      origin balance is being transferred (capped at a sensible max).
    * ``is_round_amount`` — ``1`` if ``amount`` is a "round" number
      (divisible by 1 000).
    * ``high_value_flag`` — ``1`` if ``amount`` exceeds the 95th percentile.
    * ``balance_mismatch_flag`` — ``1`` if ``|balance_error_org| > 1.0``,
      indicating a suspicious accounting discrepancy.
    * ``is_transfer_or_cashout`` — ``1`` if type in {TRANSFER, CASH_OUT},
      since most fraud happens via these channels.

    Parameters
    ----------
    enriched : pd.DataFrame
        Output of :func:`stage_enriched`.

    Returns
    -------
    pd.DataFrame
        DataFrame augmented with fraud-scoring features.
    """
    df = enriched.copy()

    # ── amount z-score ─────────────────────────────────────────────────
    amount_mean = df["amount"].mean()
    amount_std = df["amount"].std()
    if amount_std > 0:
        df["amount_zscore"] = np.round(
            (df["amount"] - amount_mean) / amount_std, 4
        )
    else:
        df["amount_zscore"] = 0.0

    # ── balance ratio ──────────────────────────────────────────────────
    df["balance_ratio"] = np.round(
        df["amount"] / (df["oldbalanceOrg"] + 1.0), 4
    )

    # ── is_round_amount ────────────────────────────────────────────────
    df["is_round_amount"] = (df["amount"] % 1000 == 0).astype(int)

    # ── high_value_flag ────────────────────────────────────────────────
    p95 = df["amount"].quantile(0.95)
    df["high_value_flag"] = (df["amount"] > p95).astype(int)

    # ── balance_mismatch_flag ──────────────────────────────────────────
    if "balance_error_org" in df.columns:
        df["balance_mismatch_flag"] = (
            df["balance_error_org"].abs() > 1.0
        ).astype(int)
    else:
        df["balance_mismatch_flag"] = 0

    # ── is_transfer_or_cashout ─────────────────────────────────────────
    df["is_transfer_or_cashout"] = df["type"].isin(
        {"TRANSFER", "CASH_OUT"}
    ).astype(int)

    logger.info(
        "stage_fraud_features: %d rows, %d columns",
        len(df),
        len(df.columns),
    )
    return df
