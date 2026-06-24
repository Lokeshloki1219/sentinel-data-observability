"""
Sentinel — Risk-Tiered Gate Policy (Spec §12).

Determines the governance gate that must be satisfied before an action
can execute.  The gate is derived from the action's default gate in the
registry, optionally *escalated* when the dataset's criticality is high
or critical.

From the spec: "gate = registry[action].gate, optionally escalated by
intent.criticality (e.g., a medium action on a critical dataset →
typed_confirmation).  blocked actions are never executed by Sentinel."
"""

from __future__ import annotations

from schemas import (
    ActionDefinition,
    Criticality,
    GateType,
    RiskTier,
)
from action.registry import is_blocked


def evaluate_gate(
    action_def: ActionDefinition,
    criticality: Criticality,
) -> GateType:
    """Determine the governance gate for a proposed action.

    The algorithm:

    1. If the action is blocked (risk_tier == blocked), return
       ``GateType.blocked`` unconditionally.
    2. Start with the action's default gate from its
       ``ActionDefinition``.
    3. If the dataset criticality is ``high`` or ``critical`` **and** the
       action's risk tier is ``medium``, escalate the gate to
       ``typed_confirmation``.
    4. If the dataset criticality is ``critical`` **and** the action's
       risk tier is ``safe``, also escalate to ``typed_confirmation``.

    Parameters
    ----------
    action_def:
        The action definition from the registry.
    criticality:
        The criticality level of the affected dataset (from
        ``IntentConfig``).

    Returns
    -------
    GateType
        The required gate: ``one_click``, ``typed_confirmation``, or
        ``blocked``.
    """
    # Rule 1: blocked actions are always blocked.
    if action_def.risk_tier == RiskTier.blocked:
        return GateType.blocked

    if is_blocked(action_def.action_type):
        return GateType.blocked

    gate = action_def.gate

    # Rule 3 & 4: escalate based on criticality.
    if criticality in (Criticality.high, Criticality.critical):
        if action_def.risk_tier == RiskTier.medium:
            gate = GateType.typed_confirmation
        elif (
            action_def.risk_tier == RiskTier.safe
            and criticality == Criticality.critical
        ):
            # Safe action on a critical dataset → escalate from
            # one_click to typed_confirmation for extra caution.
            gate = GateType.typed_confirmation

    return gate
