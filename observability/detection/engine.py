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
from typing import Dict, List, Optional, Tuple

from config import config
from schemas import (
    Anomaly,
    IntentConfig,
    OperationalSignals,
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
    check_uniqueness,
    check_validity,
    check_volume,
)
from observability.detection.operational import check_operational

logger = logging.getLogger(__name__)


# ── Public API ─────────────────────────────────────────────────────────────


def run_detection(
    metrics: RunMetrics,
    store: SentinelStore,
    intent: IntentConfig,
    op: Optional[OperationalSignals] = None,
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
    op : OperationalSignals, optional
        The stage's operational signal.  When provided, operational checks
        (OOM/timeout/slow/retry) run in the *same* pass so all anomalies for
        the (dataset, stage) share one debounce/suppress/dedup cycle.

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

    raw_anomalies.extend(check_validity(metrics, intent))

    uniq_anom = check_uniqueness(metrics, intent)
    if uniq_anom:
        raw_anomalies.append(uniq_anom)

    # Operational checks (OOM/timeout/slow/retry) — same pass so they share the
    # stage's debounce/suppress/dedup cycle.
    if op is not None:
        ops_history = store.get_recent_ops(op.job_name, config.BASELINE_WINDOW)
        raw_anomalies.extend(
            check_operational(op, ops_history, metrics.dataset, intent)
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
    """Apply debounce logic to raw anomalies (spec §11).

    *   **high / critical** → escalate immediately.
    *   **low / medium**    → escalate only after the *same metric* has been
        anomalous for ``config.DEBOUNCE_RUNS`` (default 2) consecutive runs.

    Consecutive-run state is tracked in the ``anomaly_streaks`` table rather
    than inferred from open incidents (the previous approach could never
    escalate a low/medium anomaly, because no record of a non-escalated
    anomaly was ever persisted).  Each call:

    1. increments the streak for every metric anomalous *this* run,
    2. resets the streak for metrics that were anomalous before but not now.

    All anomalies in *anomalies* are assumed to share one (dataset, stage) —
    which holds because detection runs per (run, stage).
    """
    if not anomalies:
        return []

    dataset = anomalies[0].dataset
    stage = anomalies[0].stage
    threshold = config.DEBOUNCE_RUNS

    prior_streaks = store.get_anomaly_streaks(dataset, stage)
    current_metrics = [a.metric for a in anomalies]

    # Break streaks for metrics that recovered (anomalous before, not now).
    store.clear_anomaly_streaks(dataset, stage, keep_metrics=current_metrics)

    escalated: List[Anomaly] = []
    for anomaly in anomalies:
        new_streak = prior_streaks.get(anomaly.metric, 0) + 1
        store.set_anomaly_streak(
            dataset, stage, anomaly.metric, new_streak, anomaly.run_id
        )

        if anomaly.severity_hint in (SeverityLevel.high, SeverityLevel.critical):
            anomaly.escalated = True
            escalated.append(anomaly)
        elif new_streak >= threshold:
            anomaly.escalated = True
            escalated.append(anomaly)
        else:
            logger.debug(
                "Debounced (%s/%s streak=%d < %d): %s",
                dataset,
                stage,
                new_streak,
                threshold,
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
            if not (metric_match and type_match):
                continue

            if rule.effect.value == "suppress":
                logger.info(
                    "Suppressed anomaly %s by rule %s",
                    anomaly.anomaly_id,
                    rule.rule_id,
                )
                suppressed = True
                break

            # raise_threshold: drop the anomaly unless its deviation now
            # exceeds the raised threshold carried in rule.param.
            if rule.effect.value == "raise_threshold" and rule.param is not None:
                if abs(anomaly.deviation) < rule.param:
                    logger.info(
                        "Anomaly %s below raised threshold %.3f (rule %s) — dropped",
                        anomaly.anomaly_id,
                        rule.param,
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
