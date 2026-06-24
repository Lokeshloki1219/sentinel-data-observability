"""
Sentinel — Action Preview / Dry-Run (Spec §7.8, §12).

Generates a human-readable preview of what an action *would* do before
any approval or execution takes place.  The preview includes the risk
tier, reversibility, and an estimated effect description so the human
approver can make an informed decision.

Design rule from the spec: "Preview before acting.  Always show what
the action will do (a diff / dry-run / predicted effect) before approval.
Never approve a black box."
"""

from __future__ import annotations

from dataclasses import dataclass

from schemas import ActionType, RiskTier
from action.registry import get_action


@dataclass
class PreviewResult:
    """Human-readable preview of a proposed action.

    Attributes
    ----------
    action_type:
        The kind of action being previewed.
    target:
        The target entity (job name, batch ID, etc.).
    description:
        A plain-English explanation of what the action will do.
    estimated_effect:
        A short summary of the expected outcome.
    reversible:
        Whether the action can be undone.
    risk_tier:
        The action's risk classification from the registry.
    """

    action_type: ActionType
    target: str
    description: str
    estimated_effect: str
    reversible: bool
    risk_tier: RiskTier


# ── Preview generators (one per action type) ──────────────────────────────

def _preview_rerun_job(target: str, dataset: str) -> PreviewResult:
    """Preview a job re-run."""
    return PreviewResult(
        action_type=ActionType.rerun_job,
        target=target,
        description=(
            f"Re-trigger the orchestrator job '{target}' for dataset "
            f"'{dataset}'.  The job will execute with the same configuration "
            f"as the original run."
        ),
        estimated_effect=(
            f"A new run of '{target}' will be queued.  If the original "
            f"failure was transient (network, timeout), this should produce "
            f"a successful batch and resolve the anomaly."
        ),
        reversible=True,
        risk_tier=RiskTier.safe,
    )


def _preview_quarantine_batch(target: str, dataset: str) -> PreviewResult:
    """Preview quarantining a batch."""
    return PreviewResult(
        action_type=ActionType.quarantine_batch,
        target=target,
        description=(
            f"Move the flagged batch rows identified by run '{target}' in "
            f"dataset '{dataset}' from the main table to the "
            f"'quarantine' table in the warehouse."
        ),
        estimated_effect=(
            f"Downstream consumers of '{dataset}' will no longer see the "
            f"affected rows.  The quarantined rows are preserved and can "
            f"be restored via the un-quarantine (undo) action."
        ),
        reversible=True,
        risk_tier=RiskTier.safe,
    )


def _preview_backfill(target: str, dataset: str) -> PreviewResult:
    """Preview a backfill operation."""
    return PreviewResult(
        action_type=ActionType.backfill,
        target=target,
        description=(
            f"Backfill the window '{target}' for dataset '{dataset}' by "
            f"re-ingesting from the data source.  This will overwrite the "
            f"existing batch for the specified window."
        ),
        estimated_effect=(
            f"Data for the backfill window will be re-fetched and "
            f"re-processed.  If the source data has been corrected, the "
            f"anomaly should resolve after the backfill completes."
        ),
        reversible=True,
        risk_tier=RiskTier.medium,
    )


# ── Dispatch table ────────────────────────────────────────────────────────

_PREVIEW_DISPATCH = {
    ActionType.rerun_job: _preview_rerun_job,
    ActionType.quarantine_batch: _preview_quarantine_batch,
    ActionType.backfill: _preview_backfill,
}


def preview_action(
    action_type: ActionType,
    target: str,
    dataset: str,
) -> PreviewResult:
    """Generate a human-readable preview of a proposed action.

    Parameters
    ----------
    action_type:
        The action to preview.
    target:
        The target entity — a job name for ``rerun_job``, a run/batch ID
        for ``quarantine_batch``, or a time-window identifier for
        ``backfill``.
    dataset:
        The dataset affected by the action.

    Returns
    -------
    PreviewResult
        A structured preview with description, estimated effect, risk
        tier, and reversibility.

    Raises
    ------
    ValueError
        If *action_type* has no preview generator (e.g. blocked actions).
    """
    generator = _PREVIEW_DISPATCH.get(action_type)
    if generator is None:
        # Fallback: build a generic preview from the registry definition.
        action_def = get_action(action_type)
        if action_def is None:
            raise ValueError(
                f"No preview available for action '{action_type.value}'. "
                f"This action may be blocked or unregistered."
            )
        return PreviewResult(
            action_type=action_type,
            target=target,
            description=f"Execute '{action_type.value}' on '{target}' for dataset '{dataset}'.",
            estimated_effect="Effect details are not available for this action type.",
            reversible=action_def.reversible,
            risk_tier=action_def.risk_tier,
        )
    return generator(target, dataset)
