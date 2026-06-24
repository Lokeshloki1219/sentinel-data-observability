"""
Sentinel — Detection Engine.

Orchestrates the full anomaly-detection pipeline for a single
:class:`~schemas.RunMetrics` snapshot:

1. Fetch N=30 historical runs from the store.
2. Execute every rule-based check.
3. **Debounce** — low / medium anomalies require 2 consecutive
   anomalous runs before they escalate; high / critical escalate
   immediately.
4. **Suppress** — remove anomalies matching active
   :class:`~schemas.SuppressionRule` entries.
5. **Deduplicate** — keep at most one anomaly per
   ``(dataset, metric, run_id)``.
6. Return the escalated, de-duplicated anomaly list.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from config import config
from schemas import (
    Anomaly,
    CheckType,
    IntentConfig,
    RunMetrics,
    SeverityLevel,
    SuppressionRule,
)
from observability.store import SentinelStore
from observability.detection.rules import (
    check_distribution,
    check_freshness,
    check_null_rate,
    check_schema,
    check_volume,
)

logger = logging.getLogger(__name__)


# ── Public API ─────────────────────────────────────────────────────────────


def run_detection(
    metrics: RunMetrics,
    store: SentinelStore,
    intent: IntentConfig,
) -> List[Anomaly]:
    """Execute the full detection pipeline for one run.

    Parameters
    ----------
    metrics : RunMetrics
        Freshly computed metrics for the current pipeline run.
    store : SentinelStore
        Persistence layer used to fetch history and suppression rules.
    intent : IntentConfig
        Dataset-level configuration (thresholds, criticality, key cols).

    Returns
    -------
    list[Anomaly]
        Escalated, de-duplicated anomalies ready for incident creation.
    """
    n = config.BASELINE_WINDOW  # default 30
    history: List[RunMetrics] = store.get_recent_metrics(
        metrics.dataset, metrics.stage, n
    )
    prev_metrics = history[0] if history else None

    # ── 1. Run all checks ──────────────────────────────────────────────
    raw_anomalies: List[Anomaly] = []

    freshness_anom = check_freshness(metrics, intent)
    if freshness_anom:
        raw_anomalies.append(freshness_anom)

    volume_anom = check_volume(metrics, intent, history)
    if volume_anom:
        raw_anomalies.append(volume_anom)

    raw_anomalies.extend(check_null_rate(metrics, intent, history))

    schema_anom = check_schema(metrics, prev_metrics)
    if schema_anom:
        raw_anomalies.append(schema_anom)

    raw_anomalies.extend(
        check_distribution(metrics, history, intent.key_columns)
    )

    if not raw_anomalies:
        logger.debug(
            "No anomalies detected for %s/%s run=%s",
            metrics.dataset,
            metrics.stage,
            metrics.run_id,
        )
        return []

    logger.info(
        "Detected %d raw anomalies for %s/%s run=%s",
        len(raw_anomalies),
        metrics.dataset,
        metrics.stage,
        metrics.run_id,
    )

    # ── 2. Debounce ────────────────────────────────────────────────────
    escalated = debounce(raw_anomalies, history, store)

    # ── 3. Suppress ────────────────────────────────────────────────────
    rules = store.get_active_suppressions(metrics.dataset)
    escalated = drop_suppressed(escalated, rules)

    # ── 4. Cost-control dedup ──────────────────────────────────────────
    escalated = _dedup(escalated)

    logger.info(
        "After debounce/suppress/dedup: %d escalated anomalies for %s/%s",
        len(escalated),
        metrics.dataset,
        metrics.stage,
    )
    return escalated


# ── Debounce ───────────────────────────────────────────────────────────────


def debounce(
    anomalies: List[Anomaly],
    history: List[RunMetrics],
    store: SentinelStore,
) -> List[Anomaly]:
    """Apply debounce logic to raw anomalies.

    *   **high / critical** → escalate immediately.
    *   **low / medium**    → escalate only if the *same metric* was
        also anomalous in the immediately preceding run (i.e. 2
        consecutive anomalous runs are required).

    The look-back is implemented by checking whether an open incident
    already exists for the same ``(dataset, metric)`` pair.  This avoids
    re-running detection on the previous run; the presence of an open
    incident is a reliable proxy for "was anomalous last time".

    For runs without prior incidents we fall back to a simple heuristic:
    the first occurrence of a low/medium anomaly is *not* escalated.
    """
    escalated: List[Anomaly] = []

    # Cache of previously anomalous metrics from open incidents
    open_incidents = store.get_open_incidents()
    prev_anomalous_metrics: Set[Tuple[str, str]] = set()
    for inc in open_incidents:
        for a in inc.anomalies:
            prev_anomalous_metrics.add((a.dataset, a.metric))

    for anomaly in anomalies:
        if anomaly.severity_hint in (SeverityLevel.high, SeverityLevel.critical):
            anomaly.escalated = True
            escalated.append(anomaly)
        else:
            key = (anomaly.dataset, anomaly.metric)
            if key in prev_anomalous_metrics:
                anomaly.escalated = True
                escalated.append(anomaly)
            else:
                logger.debug(
                    "Debounced (first occurrence): %s / %s",
                    anomaly.dataset,
                    anomaly.metric,
                )

    return escalated


# ── Suppression ────────────────────────────────────────────────────────────


def drop_suppressed(
    anomalies: List[Anomaly],
    rules: List[SuppressionRule],
) -> List[Anomaly]:
    """Remove anomalies that match an active suppression rule.

    A rule matches when **all** of the following are true:

    * ``rule.match.metric`` equals ``anomaly.metric``  (or is ``"*"``).
    * ``rule.match.check_type`` equals ``anomaly.check_type.value``
      (or is ``"*"``).
    * ``rule.effect`` is ``suppress``.
    """
    if not rules:
        return anomalies

    kept: List[Anomaly] = []
    for anomaly in anomalies:
        suppressed = False
        for rule in rules:
            metric_match = (
                rule.match.metric == anomaly.metric or rule.match.metric == "*"
            )
            type_match = (
                rule.match.check_type == anomaly.check_type.value
                or rule.match.check_type == "*"
            )
            if metric_match and type_match and rule.effect.value == "suppress":
                logger.info(
                    "Suppressed anomaly %s by rule %s",
                    anomaly.anomaly_id,
                    rule.rule_id,
                )
                suppressed = True
                break
        if not suppressed:
            kept.append(anomaly)

    return kept


# ── Deduplication ──────────────────────────────────────────────────────────


def _dedup(anomalies: List[Anomaly]) -> List[Anomaly]:
    """Keep at most one anomaly per ``(dataset, metric, run_id)``.

    When duplicates exist the one with the highest severity wins.
    """
    _SEVERITY_RANK: Dict[SeverityLevel, int] = {
        SeverityLevel.low: 0,
        SeverityLevel.medium: 1,
        SeverityLevel.high: 2,
        SeverityLevel.critical: 3,
    }

    best: Dict[Tuple[str, str, str], Anomaly] = {}
    for a in anomalies:
        key = (a.dataset, a.metric, a.run_id)
        existing = best.get(key)
        if existing is None or _SEVERITY_RANK.get(
            a.severity_hint, 0
        ) > _SEVERITY_RANK.get(existing.severity_hint, 0):
            best[key] = a
    return list(best.values())


# ── Grouping ───────────────────────────────────────────────────────────────


def group_related(anomalies: List[Anomaly]) -> List[List[Anomaly]]:
    """Group anomalies by ``(dataset, stage, run_id)``.

    Multiple metric deviations from the same dataset / stage / run are
    bundled into a single group so the downstream incident-creation step
    can treat them as one logical event.

    Returns
    -------
    list[list[Anomaly]]
        Each inner list is a group that will become one
        :class:`~schemas.Incident`.
    """
    groups: Dict[Tuple[str, str, str], List[Anomaly]] = defaultdict(list)
    for a in anomalies:
        groups[(a.dataset, a.stage, a.run_id)].append(a)
    return list(groups.values())
