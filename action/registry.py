"""
Sentinel — Action Registry (Spec §7.8, §12).

Declares every known action, its risk tier, reversibility, and the
governance gate that must be passed before execution.  Blocked actions
are documented here but deliberately excluded from the executable
registry so the executor can never run them.
"""

from __future__ import annotations

from typing import Dict, Optional

from schemas import (
    ActionDefinition,
    ActionType,
    GateType,
    RiskTier,
)

# ── Executable action registry ────────────────────────────────────────────
# Only actions listed here can ever be proposed or executed by Sentinel.

ACTION_REGISTRY: Dict[ActionType, ActionDefinition] = {
    ActionType.rerun_job: ActionDefinition(
        action_type=ActionType.rerun_job,
        risk_tier=RiskTier.safe,
        reversible=True,
        gate=GateType.one_click,
    ),
    ActionType.quarantine_batch: ActionDefinition(
        action_type=ActionType.quarantine_batch,
        risk_tier=RiskTier.safe,
        reversible=True,
        gate=GateType.one_click,
    ),
    ActionType.backfill: ActionDefinition(
        action_type=ActionType.backfill,
        risk_tier=RiskTier.medium,
        reversible=True,
        gate=GateType.typed_confirmation,
    ),
}

# ── Blocked actions ───────────────────────────────────────────────────────
# These are explicitly out of scope (spec §3, §12).  They are NOT in the
# registry and Sentinel will never propose or execute them.
#
#   patch_data   — risk_tier=blocked, reversible=False, gate=blocked
#   alter_schema — risk_tier=blocked, reversible=False, gate=blocked
#
# We keep a set so governance/policy can quickly confirm an action is
# blocked without needing a full ActionDefinition.

BLOCKED_ACTIONS: frozenset[ActionType] = frozenset()
# ActionType enum only has rerun_job, quarantine_batch, backfill, none,
# manual — patch_data and alter_schema are not enum members because they
# must never enter the system.  We document them as string constants:

BLOCKED_ACTION_NAMES: frozenset[str] = frozenset({"patch_data", "alter_schema"})
"""
Action names that are categorically blocked.  Since these are never valid
``ActionType`` enum values, they can only arrive as free-form strings from
an LLM or user input — and must be rejected at the boundary.
"""


def get_action(action_type: ActionType) -> Optional[ActionDefinition]:
    """Look up an action definition from the registry.

    Parameters
    ----------
    action_type:
        The action to look up.

    Returns
    -------
    ActionDefinition | None
        The definition if the action is registered, otherwise ``None``.
    """
    return ACTION_REGISTRY.get(action_type)


def is_blocked(action_type: ActionType) -> bool:
    """Return ``True`` if *action_type* is explicitly blocked.

    An action is considered blocked when:
    1. It appears in :data:`BLOCKED_ACTIONS`, **or**
    2. It is not in the executable :data:`ACTION_REGISTRY` and is not one
       of the "no-op" types (``none``, ``manual``).

    The second rule is a safety net: if a new action type is added to the
    enum but not yet registered, Sentinel treats it as blocked until an
    engineer explicitly adds it to the registry.
    """
    if action_type in BLOCKED_ACTIONS:
        return True
    if action_type in (ActionType.none, ActionType.manual):
        return False  # These are "do nothing" types, not blocked.
    return action_type not in ACTION_REGISTRY
