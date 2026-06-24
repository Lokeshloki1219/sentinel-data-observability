"""
Sentinel — Intent Configuration Parser (Section 7.1 / 8).

Loads YAML intent files from ``intent/datasets/`` and returns validated
``IntentConfig`` Pydantic models.  This module is the sole entry-point
for reading intent; no other module should parse intent YAML directly.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict

import yaml

from schemas import IntentConfig, ExpectedVolume
from config import config

logger = logging.getLogger(__name__)


def _intent_dir() -> Path:
    """Return the resolved path to the intent datasets directory."""
    return config.INTENT_DIR


def _parse_yaml(path: Path) -> IntentConfig:
    """Parse a single YAML file and return a validated IntentConfig.

    Parameters
    ----------
    path : Path
        Absolute or relative path to a ``.yaml`` intent file.

    Returns
    -------
    IntentConfig
        Validated Pydantic model.

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    yaml.YAMLError
        If the file is not valid YAML.
    pydantic.ValidationError
        If the parsed dict does not satisfy the ``IntentConfig`` schema.
    """
    with open(path, "r", encoding="utf-8") as fh:
        raw: dict = yaml.safe_load(fh)

    # Convert nested expected_volume dict → ExpectedVolume model if present
    if "expected_volume" in raw and isinstance(raw["expected_volume"], dict):
        raw["expected_volume"] = ExpectedVolume(**raw["expected_volume"])

    intent = IntentConfig(**raw)
    logger.info("Loaded intent for dataset=%s from %s", intent.dataset, path)
    return intent


def load_intent(dataset: str) -> IntentConfig:
    """Load the intent configuration for a specific dataset.

    Parameters
    ----------
    dataset : str
        Name of the dataset (must match the YAML filename without extension,
        e.g. ``"transactions"`` → ``intent/datasets/transactions.yaml``).

    Returns
    -------
    IntentConfig
        Validated intent configuration for the requested dataset.

    Raises
    ------
    FileNotFoundError
        If ``intent/datasets/{dataset}.yaml`` does not exist.
    """
    path = _intent_dir() / f"{dataset}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"Intent file not found for dataset '{dataset}': {path}"
        )
    return _parse_yaml(path)


def load_all_intents() -> Dict[str, IntentConfig]:
    """Load every ``*.yaml`` file in the intent datasets directory.

    Returns
    -------
    Dict[str, IntentConfig]
        Mapping from dataset name → validated ``IntentConfig``.
        The dataset name is taken from the ``dataset`` field inside the YAML
        (not from the filename) for consistency.

    Raises
    ------
    FileNotFoundError
        If the intent datasets directory does not exist.
    """
    intent_dir = _intent_dir()
    if not intent_dir.exists():
        raise FileNotFoundError(
            f"Intent datasets directory not found: {intent_dir}"
        )

    intents: Dict[str, IntentConfig] = {}
    for yaml_path in sorted(intent_dir.glob("*.yaml")):
        try:
            intent = _parse_yaml(yaml_path)
            intents[intent.dataset] = intent
        except Exception:
            logger.exception("Failed to parse intent file %s", yaml_path)
            raise

    logger.info("Loaded %d intent configuration(s)", len(intents))
    return intents
