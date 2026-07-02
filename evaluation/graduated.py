"""
Sentinel — Graduated-severity evaluation (§15, hardened).

The headline `run_experiments` deliberately injects *obvious* faults (50% row
drop, 10x shift) — a z>=3 detector cannot miss those, so F1 is trivially 1.0.
This module tells the honest story instead: it injects each fault family at a
**range of magnitudes** against a baseline with **realistic volume variance**,
and reports

  1. a per-magnitude detection table (recall degrades as faults get subtler),
  2. a precision / recall / F1 vs. **z-threshold sweep** on the volume score,

so the operating point (z>=3) is a visible trade-off, not a magic number.

Run:  python -m evaluation.graduated
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

from intent.parser import load_intent
from observability.store import SentinelStore
from observability.metrics import compute_metrics
from observability.detection.engine import run_detection
from observability.detection.statistical import compute_zscore
from pipeline.ingest import generate_batch
from pipeline.faults import FaultSpec, inject_fault
from evaluation.detection_metrics import expected_checks_for

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("graduated")

STAGE = "raw_transactions"
BASELINE_N = 24          # clean baseline runs
CLEAN_TEST_N = 15        # held-out clean runs (negatives for the sweep)
BASE_ROWS = 10_000
VOL_SIGMA = 200          # ~2% natural run-to-run volume variance

# Graduated magnitudes per family: (label, params, severity bucket).
VOLUME_DROPS = [0.40, 0.30, 0.20, 0.15, 0.10, 0.08, 0.05, 0.03, 0.02]
NULL_RATES = [0.10, 0.05, 0.03, 0.02, 0.015, 0.008]     # oldbalanceOrg SLA = 0.01
DIST_FACTORS = [10.0, 3.0, 2.0, 1.5, 1.2, 1.1, 1.05]

# What "should" be caught, for a per-severity (obvious/moderate/subtle) view.
def _sev_bucket(z_or_ratio: float) -> str:
    a = abs(z_or_ratio)
    if a >= 5: return "obvious"
    if a >= 3: return "moderate"
    return "subtle"


def _clean_batch(day: int, rng: np.random.Generator):
    nrows = max(int(rng.normal(BASE_ROWS, VOL_SIGMA)), 500)
    return generate_batch(day, num_rows=nrows)


def _metrics(store: SentinelStore, intent, batch, run_id: str):
    return compute_metrics(run_id, "transactions", STAGE, batch,
                           intent.key_columns, unique_key=intent.unique_key)


def run_graduated(output_path: str = "data/graduated_eval.json", verbose: bool = True) -> Dict:
    rng = np.random.default_rng(0)
    store = SentinelStore(":memory:")
    intent = load_intent("transactions")

    # ── Baseline (with realistic volume variance) ──────────────────────
    baseline_counts: List[float] = []
    for day in range(BASELINE_N):
        b = _clean_batch(day, rng)
        m = _metrics(store, intent, b, f"base-{day}")
        store.save_metrics(m)
        baseline_counts.append(float(m.row_count))
    base_mean = float(np.mean(baseline_counts))
    base_std = float(np.std(baseline_counts, ddof=1))
    logger.warning("baseline volume: mean=%.0f std=%.0f", base_mean, base_std)

    results: Dict = {
        "baseline": {"runs": BASELINE_N, "mean_rows": round(base_mean, 1),
                     "std_rows": round(base_std, 1), "vol_sigma": VOL_SIGMA},
        "graduated_detection": {},
        "threshold_sweep": [],
        "per_severity_recall": {},
    }

    def detect(fault: FaultSpec, day: int):
        """Inject on a fresh clean batch and return (matched, checks, severity)."""
        clean = generate_batch(day, num_rows=BASE_ROWS)
        corrupted, _label = inject_fault(clean, fault)
        m = _metrics(store, intent, corrupted, f"fault-{day}")
        anoms = run_detection(m, store, intent)          # detect (not persisted)
        exp = expected_checks_for(fault.fault_type)
        matched = [a for a in anoms if a.check_type.value in exp]
        sev = max((a.severity_hint.value for a in matched), default=None)
        return bool(matched), sorted({a.check_type.value for a in anoms}), sev, m

    day = 1000
    grad = results["graduated_detection"]

    # ── Volume family (continuous z-score → the star of the sweep) ─────
    vol_rows, sweep_pos = [], []
    grad["volume"] = []
    for d in VOLUME_DROPS:
        matched, checks, sev, m = detect(
            FaultSpec("row_drop", "", {"drop_pct": d}, seed=day), day)
        z = compute_zscore(float(m.row_count), baseline_counts)
        sweep_pos.append(z)
        grad["volume"].append({"drop_pct": d, "rows": m.row_count,
                               "z": round(z, 2), "detected": matched,
                               "severity": sev})
        day += 1

    # ── Null family (hard SLA threshold on oldbalanceOrg) ──────────────
    grad["null_rate"] = []
    for r in NULL_RATES:
        matched, checks, sev, _ = detect(
            FaultSpec("column_null", "oldbalanceOrg", {"null_pct": r}, seed=day), day)
        grad["null_rate"].append({"null_pct": r, "detected": matched, "severity": sev})
        day += 1

    # ── Distribution family (median z-score) ───────────────────────────
    grad["distribution"] = []
    for f in DIST_FACTORS:
        matched, checks, sev, _ = detect(
            FaultSpec("distribution_shift", "amount", {"factor": f}, seed=day), day)
        grad["distribution"].append({"factor": f, "detected": matched, "severity": sev})
        day += 1

    # ── Threshold sweep on the volume z-score ──────────────────────────
    neg_z = []
    for i in range(CLEAN_TEST_N):
        b = generate_batch(5000 + i, num_rows=max(int(rng.normal(BASE_ROWS, VOL_SIGMA)), 500))
        m = _metrics(store, intent, b, f"neg-{i}")
        neg_z.append(compute_zscore(float(m.row_count), baseline_counts))

    for t in [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]:
        tp = sum(1 for z in sweep_pos if abs(z) >= t)
        fn = len(sweep_pos) - tp
        fp = sum(1 for z in neg_z if abs(z) >= t)
        tn = len(neg_z) - fp
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
        results["threshold_sweep"].append({
            "z_threshold": t, "precision": round(prec, 3),
            "recall": round(rec, 3), "f1": round(f1, 3),
            "tp": tp, "fp": fp, "fn": fn, "tn": tn})

    # ── Per-severity recall (obvious / moderate / subtle by |z|) ────────
    buckets: Dict[str, List[bool]] = {"obvious": [], "moderate": [], "subtle": []}
    for row in grad["volume"]:
        buckets[_sev_bucket(row["z"])].append(row["detected"])
    results["per_severity_recall"] = {
        k: {"n": len(v), "recall": round(sum(v) / len(v), 3) if v else None}
        for k, v in buckets.items()
    }

    best = max(results["threshold_sweep"], key=lambda s: s["f1"])
    results["best_f1"] = {"z_threshold": best["z_threshold"], "f1": best["f1"]}

    # Per-family recall across the graduated magnitudes (completes the story
    # beyond volume: null degrades at its 0.01 SLA, distribution near 1.1x).
    results["family_recall"] = {
        fam: round(sum(1 for r in rows if r["detected"]) / len(rows), 3)
        for fam, rows in grad.items() if rows
    }

    # ── Print a readable summary ───────────────────────────────────────
    if verbose:
        print("\n=== Graduated volume detection (baseline std ~%.0f rows) ===" % base_std)
        for row in grad["volume"]:
            print(f"  drop {int(row['drop_pct']*100):>2}%  rows={row['rows']:>5}  "
                  f"z={row['z']:>7.2f}  {'DETECTED' if row['detected'] else 'missed  '}  {row['severity'] or ''}")
        print("\n=== Precision/Recall/F1 vs z-threshold (volume) ===")
        print("   t     precision  recall   f1     (tp/fp/fn)")
        for s in results["threshold_sweep"]:
            print(f"  {s['z_threshold']:>3}     {s['precision']:.2f}      {s['recall']:.2f}    "
                  f"{s['f1']:.2f}    ({s['tp']}/{s['fp']}/{s['fn']})")
        print("\n=== Recall by severity bucket (volume) ===")
        for k, v in results["per_severity_recall"].items():
            print(f"  {k:9s} n={v['n']}  recall={v['recall']}")
        print(f"\nBest F1 at z={best['z_threshold']} (F1={best['f1']}); "
              f"the shipped detector uses z>=3.0.")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    if verbose:
        print(f"\nSaved -> {output_path}")
    return results


def run_suppression_demo(output_path: str = "data/suppression_demo.json", verbose: bool = True) -> Dict:
    """Exercise the reject->suppression->FP-drop loop for real (no LLM).

    A benign but recurring +25% volume surge trips the volume check every run
    (a genuine false positive). After one is labelled `not_a_problem` — creating
    a SuppressionRule via the real governance path — the same pattern is dropped
    on every subsequent run, so the false-positive rate for it falls 1.0 -> 0.0.
    """
    from datetime import datetime, timezone
    from schemas import Incident, IncidentStatus
    from governance.suppression import create_suppression_from_incident

    store = SentinelStore(":memory:")
    intent = load_intent("transactions")

    # Fixed clean baseline at ~BASE_ROWS (kept fixed: surge runs are detected but
    # not persisted, so they never shift the baseline).
    for d in range(BASELINE_N):
        store.save_metrics(_metrics(store, intent, generate_batch(d, num_rows=BASE_ROWS), f"cb-{d}"))

    def benign_surge(day: int):
        m = _metrics(store, intent, generate_batch(day, num_rows=int(BASE_ROWS * 1.25)), f"surge-{day}")
        vol = [a for a in run_detection(m, store, intent) if a.check_type.value == "volume"]
        return vol, m

    fp_trend: List[int] = []

    # Run 1 — the benign surge fires (a false positive).
    vol, m = benign_surge(3000)
    fp_trend.append(1 if vol else 0)

    # Human labels it not_a_problem → SuppressionRule created (real governance path).
    inc = Incident(incident_id="sup-demo", created_at=datetime.now(timezone.utc),
                   dataset="transactions", stage=STAGE, run_id=m.run_id,
                   anomalies=vol, status=IncidentStatus.open)
    rule = create_suppression_from_incident(inc, store)

    # Runs 2-6 — same surge, now suppressed → no false positive.
    for day in range(3001, 3006):
        vol, _ = benign_surge(day)
        fp_trend.append(1 if vol else 0)

    result = {
        "fp_trend": fp_trend,
        "suppression_rule": {"metric": rule.match.metric, "check_type": rule.match.check_type},
        "fp_rate_before": fp_trend[0],
        "fp_rate_after": round(sum(fp_trend[1:]) / len(fp_trend[1:]), 3),
    }
    if verbose:
        print("\n=== Suppression loop (benign +25% volume surge) ===")
        print(f"  FP trend per run: {fp_trend}   (1 = false positive fired)")
        print(f"  Before labelling: {result['fp_rate_before']}  ->  after not_a_problem: {result['fp_rate_after']}")
        print(f"  Rule: suppress {rule.match.check_type} on '{rule.match.metric}'")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    return result


if __name__ == "__main__":
    run_graduated()
    run_suppression_demo()

