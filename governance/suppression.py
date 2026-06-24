"""
Sentinel — Suppression Rule Management (Spec §7.11, §12).

Handles creation and matching of suppression rules.  When a human marks
an incident as ``not_a_problem``, a ``SuppressionRule`` is created so
that identical anomaly patterns on the same dataset are silently dropped
(or have their thresholds raised) in subsequent detection runs.

From the spec: "a not_a_problem resolution creates a SuppressionRule;
detection consults active rules and drops/raises thresholds for matching
anomalies before escalation."
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import List

from schemas import (
    Anomaly,
    Incident,
    SuppressionEffect,
    SuppressionMatch,
    SuppressionRule,
)


def create_suppression_from_incident(
    incident: Incident,
    store,
) -> SuppressionRule:
    """Create and persist a suppression rule derived from an incident.

    The rule matches the *first* anomaly's metric and check_type on the
    incident's dataset.  If the incident has multiple anomalies, the
    caller should decide whether to create one rule per anomaly or a
    single rule for the primary one — this function covers the common
    single-anomaly case.

    Parameters
    ----------
    incident:
        The incident that was resolved as ``not_a_problem``.
    store:
        The ``SentinelStore`` (or any object exposing
        ``save_suppression_rule(rule)``).

    Returns
    -------
    SuppressionRule
        The newly created, persisted rule.

    Raises
    ------
    ValueError
        If the incident has no anomalies to derive a rule from.
    """
    if not incident.anomalies:
        raise ValueError(
            f"Cannot create suppression rule from incident "
            f"'{incident.incident_id}': no anomalies attached."
        )

    primary_anomaly = incident.anomalies[0]

    rule = SuppressionRule(
        rule_id=str(uuid.uuid4()),
        dataset=incident.dataset,
        match=SuppressionMatch(
            metric=primary_anomaly.metric,
            check_type=primary_anomaly.check_type.value,
            condition="",  # exact match on metric + check_type
        ),
        effect=SuppressionEffect.suppress,
        param=None,
        created_from_incident=incident.incident_id,
        created_at=datetime.utcnow(),
    )

    store.save_suppression_rule(rule)
    return rule


def matches_suppression(
    anomaly: Anomaly,
    rules: List[SuppressionRule],
) -> bool:
    """Check whether an anomaly matches any active suppression rule.

    A match requires:
    - Same ``dataset``.
    - Same ``metric`` (the rule's ``match.metric``).
    - Same ``check_type`` (the rule's ``match.check_type``).

    The optional ``condition`` field on the rule is reserved for future
    conditional logic (e.g. time-of-day filters) but is not evaluated
    in this implementation.

    Parameters
    ----------
    anomaly:
        The anomaly to check.
    rules:
        The list of active suppression rules to match against.

    Returns
    -------
    bool
        ``True`` if the anomaly matches at least one rule.
    """
    for rule in rules:
        if (
            rule.dataset == anomaly.dataset
            and rule.match.metric == anomaly.metric
            and rule.match.check_type == anomaly.check_type.value
        ):
            return True
    return False
