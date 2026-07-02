"""
Sentinel — Evaluation plots.

Reads ``data/graduated_eval.json`` (produced by ``evaluation.graduated``) and
renders two static PNGs into ``docs/`` for the README:

* ``docs/threshold_curve.png``   — precision / recall / F1 vs. the z-threshold.
* ``docs/degradation_curve.png`` — detection outcome vs. fault magnitude, per family.

Run:  python -m evaluation.plots
Needs matplotlib (``pip install matplotlib`` or the ``viz`` extra).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import matplotlib
matplotlib.use("Agg")  # headless / CI-safe
import matplotlib.pyplot as plt

_GREEN, _AMBER, _RED, _BLUE = "#22c55e", "#f59e0b", "#ef4444", "#3b82f6"


def _threshold_curve(d: dict, out: Path) -> None:
    sweep = d["threshold_sweep"]
    ts = [s["z_threshold"] for s in sweep]
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    ax.plot(ts, [s["precision"] for s in sweep], "-o", color=_GREEN, label="precision")
    ax.plot(ts, [s["recall"] for s in sweep], "-o", color=_BLUE, label="recall")
    ax.plot(ts, [s["f1"] for s in sweep], "-o", color=_RED, label="F1")
    ax.axvline(3.0, ls="--", color="#94a3b8", lw=1)
    ax.annotate("shipped z=3.0\n(precision-favoured)", xy=(3.0, 0.67),
                xytext=(3.3, 0.45), fontsize=9, color="#475569",
                arrowprops=dict(arrowstyle="->", color="#94a3b8"))
    ax.set_xlabel("z-score threshold"); ax.set_ylabel("score")
    ax.set_title("Volume detection: precision / recall / F1 vs. threshold")
    ax.set_ylim(0, 1.05); ax.grid(alpha=0.25); ax.legend(loc="lower left")
    fig.tight_layout(); fig.savefig(out, dpi=130); plt.close(fig)


def _degradation_curve(d: dict, out: Path) -> None:
    grad = d["graduated_detection"]
    fams = [("volume", "drop_pct", "row-drop fraction"),
            ("null_rate", "null_pct", "null fraction (SLA 0.01)"),
            ("distribution", "factor", "shift factor")]
    fig, axes = plt.subplots(1, 3, figsize=(12.5, 3.6))
    for ax, (fam, key, xlabel) in zip(axes, fams):
        rows = grad.get(fam, [])
        xs = [r[key] for r in rows]
        det = [1 if r["detected"] else 0 for r in rows]
        colors = [_GREEN if v else _RED for v in det]
        ax.scatter(xs, det, c=colors, s=70, zorder=3, edgecolors="white")
        ax.plot(xs, det, color="#cbd5e1", lw=1, zorder=1)
        if fam == "null_rate":
            ax.axvline(0.01, ls="--", color="#94a3b8", lw=1, label="SLA")
            ax.legend(fontsize=8)
        ax.set_title(fam); ax.set_xlabel(xlabel)
        ax.set_yticks([0, 1]); ax.set_yticklabels(["missed", "detected"])
        ax.grid(alpha=0.2)
    fig.suptitle("Graceful degradation: detection outcome vs. fault magnitude "
                 "(green = detected, red = missed)", fontsize=11)
    fig.tight_layout(rect=(0, 0, 1, 0.94)); fig.savefig(out, dpi=130); plt.close(fig)


def _flow_preview(out: Path) -> None:
    """Static preview of the animated Pipeline Flow tab (stand-in for a GIF)."""
    from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
    fig, ax = plt.subplots(figsize=(11, 3.4))
    ax.set_xlim(0, 12); ax.set_ylim(0, 4); ax.axis("off")

    nodes = [("PaySim\nsource", 0.6, _GREEN), ("raw", 2.6, _GREEN),
             ("cleaned", 4.6, _GREEN), ("enriched", 6.6, _RED),
             ("fraud_features", 8.7, _AMBER), ("DuckDB\nwarehouse", 10.9, _GREEN)]
    cy = 2.6
    for label, x, color in nodes:
        ax.add_patch(FancyBboxPatch((x - 0.7, cy - 0.35), 1.4, 0.7,
                     boxstyle="round,pad=0.02", fc="#111827", ec=color, lw=2.2))
        ax.text(x, cy, label, ha="center", va="center", color="#e2e8f0",
                fontsize=9, family="monospace")
    for i in range(len(nodes) - 1):
        x0, x1 = nodes[i][1] + 0.7, nodes[i + 1][1] - 0.7
        ax.add_patch(FancyArrowPatch((x0, cy), (x1, cy), arrowstyle="->",
                     mutation_scale=14, color="#475569", lw=1.6))
    # detection node + taps
    ax.add_patch(FancyBboxPatch((5.0, 0.4), 3.0, 0.6, boxstyle="round,pad=0.02",
                 fc="#111827", ec=_RED, lw=2))
    ax.text(6.5, 0.7, "DETECTION", ha="center", va="center", color="#e2e8f0",
            fontsize=9, family="monospace")
    for x in (6.6, 8.7):
        ax.add_patch(FancyArrowPatch((x, cy - 0.35), (6.5 + (x - 6.6) * 0.3, 1.0),
                     arrowstyle="->", mutation_scale=10, color="#64748b", lw=1, ls=":"))
    # caused-by arc
    ax.add_patch(FancyArrowPatch((6.6, cy + 0.35), (8.7, cy + 0.35),
                 connectionstyle="arc3,rad=-0.4", arrowstyle="->",
                 mutation_scale=14, color=_RED, lw=1.8, ls="--"))
    ax.text(7.65, cy + 1.05, "caused-by", ha="center", color=_RED, fontsize=8)
    ax.text(0.2, 3.7, "🔴 pipeline error (OOM)   🟠 data error (volume drop)   "
            "→ correlation", fontsize=9, color="#94a3b8")
    fig.patch.set_facecolor("#0e1117")
    fig.savefig(out, dpi=130, facecolor="#0e1117"); plt.close(fig)


def make_plots(json_path: str = "data/graduated_eval.json", out_dir: str = "docs") -> None:
    d = json.loads(Path(json_path).read_text())
    out = Path(out_dir); out.mkdir(parents=True, exist_ok=True)
    _threshold_curve(d, out / "threshold_curve.png")
    _degradation_curve(d, out / "degradation_curve.png")
    _flow_preview(out / "flow_preview.png")
    print(f"Wrote threshold_curve.png, degradation_curve.png, flow_preview.png to {out}")


if __name__ == "__main__":
    make_plots()
