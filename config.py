"""
Sentinel — Environment configuration loader.

Reads .env file and provides typed access to all configuration values.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from functools import lru_cache

from dotenv import load_dotenv

# Load .env from project root
_PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(_PROJECT_ROOT / ".env")


class Config:
    """Centralised configuration."""

    # API Keys
    ANTHROPIC_API_KEY: str = os.getenv("ANTHROPIC_API_KEY", "")
    ANTHROPIC_MODEL: str = "claude-sonnet-4-6"

    # Routing
    SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")

    # Database
    DB_URL: str = os.getenv("SENTINEL_DB_URL", str(_PROJECT_ROOT / "data" / "sentinel.duckdb"))

    # Environment
    ENV: str = os.getenv("SENTINEL_ENV", "dev")

    # Detection defaults
    BASELINE_WINDOW: int = 30           # N past runs for rolling baseline
    MIN_BASELINE: int = 5               # min history before statistical checks fire
    DEBOUNCE_RUNS: int = 2              # consecutive anomalous runs to escalate low/medium
    AUTO_RESOLVE_K: int = 3             # K runs to confirm fix
    MEMORY_TOP_K: int = 5               # similar incidents to retrieve

    # PaySim time axis: `step` is an hourly index. We anchor it to a fixed
    # epoch so a batch's business event-time (and therefore freshness) can be
    # derived deterministically from the data itself.
    STEP_EPOCH: datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Paths
    PROJECT_ROOT: Path = _PROJECT_ROOT
    DATA_DIR: Path = _PROJECT_ROOT / "data"
    INTENT_DIR: Path = _PROJECT_ROOT / "intent" / "datasets"
    CHROMA_DIR: Path = _PROJECT_ROOT / "chroma_data"

    @staticmethod
    @lru_cache(maxsize=1)
    def code_version() -> str:
        """Return current git SHA; stub if not in a git repo."""
        try:
            sha = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=str(_PROJECT_ROOT),
                stderr=subprocess.DEVNULL,
            )
            return sha.decode().strip()[:12]
        except Exception:
            return "unknown"


config = Config()
