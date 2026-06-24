"""
Sentinel — Demo Seeder.

Populates the **live** warehouse (``config.DB_URL``) and vector memory
(``config.CHROMA_DIR``) by running a stretch of clean pipeline runs followed by
the labelled fault scenarios through the real control loop
(:func:`orchestrator.process_run`).  After running this, launch the dashboard:

    python scripts/seed_demo.py
    streamlit run dashboard/app.py

The Health Timeline, Incident feed (with Approve/Reject/Modify + preview), and
Audit Log will all be populated.

No API key is required by default (incidents carry a rules-only severity).  Pass
``--use-llm`` to generate full LLM root-cause reports (needs ``ANTHROPIC_API_KEY``)
and ``--fresh`` to wipe the existing live DB/memory first.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

# Ensure project root is importable when run as `python scripts/seed_demo.py`.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config import config
from pipeline.flows import run_pipeline
from intent.parser import load_intent
from observability.store import SentinelStore
from memory.store import MemoryStore
from orchestrator import process_run
from evaluation.run_experiments import FAULT_SCENARIOS

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
logger = logging.getLogger("seed_demo")


def seed(num_clean_runs: int = 12, use_llm: bool = False, fresh: bool = False) -> None:
    """Run clean + fault cycles into the live store so the dashboard has data."""
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)

    if fresh:
        db = Path(config.DB_URL)
        if db.exists():
            db.unlink()
            logger.info("Removed existing DB %s", db)
        if config.CHROMA_DIR.exists():
            shutil.rmtree(config.CHROMA_DIR, ignore_errors=True)
            logger.info("Removed existing memory dir %s", config.CHROMA_DIR)

    store = SentinelStore(config.DB_URL)
    memory = MemoryStore(str(config.CHROMA_DIR))
    intent = load_intent("transactions")

    reporter = None
    if use_llm:
        from reasoning.reporter import Reporter
        reporter = Reporter()
        logger.info("LLM reporting enabled (model=%s)", config.ANTHROPIC_MODEL)

    slack = config.SLACK_WEBHOOK_URL  # empty string disables routing

    # ── Phase 1: clean baseline (builds history + health timeline) ──────────
    logger.info("Seeding %d clean runs...", num_clean_runs)
    for day in range(num_clean_runs):
        manifest = run_pipeline(day)
        process_run(manifest, store, intent, memory_store=memory,
                    reporter=reporter, slack_webhook=slack, auto_resolve=False)

    # ── Phase 2: labelled faults (creates incidents to triage) ──────────────
    logger.info("Injecting %d fault scenarios...", len(FAULT_SCENARIOS))
    incident_total = 0
    for i, (fault_spec, _cause) in enumerate(FAULT_SCENARIOS):
        manifest = run_pipeline(num_clean_runs + i, fault_spec=fault_spec)
        incidents = process_run(manifest, store, intent, memory_store=memory,
                                reporter=reporter, slack_webhook=slack, auto_resolve=False)
        incident_total += len(incidents)
        logger.info("  %-18s -> %d incident(s)", fault_spec.fault_type, len(incidents))

    logger.info(
        "Done. Seeded %d incidents into %s. Launch:  streamlit run dashboard/app.py",
        incident_total, config.DB_URL,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed the live Sentinel DB for the dashboard.")
    parser.add_argument("--clean-runs", type=int, default=12)
    parser.add_argument("--use-llm", action="store_true", help="Generate LLM reports (needs ANTHROPIC_API_KEY).")
    parser.add_argument("--fresh", action="store_true", help="Wipe the live DB/memory before seeding.")
    args = parser.parse_args()

    seed(num_clean_runs=args.clean_runs, use_llm=args.use_llm, fresh=args.fresh)
