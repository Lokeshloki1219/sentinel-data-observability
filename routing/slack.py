"""
Sentinel — Slack Webhook Routing (Spec §8, §13).

Sends incident reports to a Slack channel via an incoming webhook for
high- and critical-severity incidents.  Uses Slack Block Kit formatting
for a rich, scannable message.

From the spec: "routing/slack.py — Sends incident reports for
high/critical severity."
"""

from __future__ import annotations

import logging
from typing import Optional

import requests

from schemas import Incident, SeverityLevel

logger = logging.getLogger(__name__)

# Severities that trigger a Slack notification.
_NOTIFY_SEVERITIES: frozenset[SeverityLevel] = frozenset(
    {SeverityLevel.high, SeverityLevel.critical}
)


def _build_blocks(incident: Incident) -> list[dict]:
    """Build Slack Block Kit blocks for an incident report.

    Returns a list of block dicts ready for the ``blocks`` field of the
    Slack ``chat.postMessage`` / webhook payload.
    """
    report = incident.report
    severity = report.severity.value.upper() if report else "UNKNOWN"
    root_cause = report.likely_root_cause if report else "N/A"
    confidence = f"{report.confidence:.0%}" if report else "N/A"
    action_type = report.suggested_action.type.value if report else "none"
    action_target = report.suggested_action.target if report else ""
    evidence_lines = report.evidence if report else []

    # Severity → emoji mapping for quick visual scanning.
    emoji = ":rotating_light:" if severity == "CRITICAL" else ":warning:"

    blocks: list[dict] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"{emoji} Sentinel Incident — {severity}",
                "emoji": True,
            },
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*Dataset:*\n`{incident.dataset}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Stage:*\n`{incident.stage}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Severity:*\n{severity}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*Confidence:*\n{confidence}",
                },
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Likely Root Cause:*\n{root_cause}",
            },
        },
    ]

    # Evidence bullets (if any).
    if evidence_lines:
        evidence_text = "\n".join(f"• {e}" for e in evidence_lines[:5])
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Evidence:*\n{evidence_text}",
                },
            }
        )

    # Suggested action.
    action_text = f"`{action_type}`"
    if action_target:
        action_text += f" → `{action_target}`"
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Suggested Action:*\n{action_text}",
            },
        }
    )

    # Context footer.
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"Incident `{incident.incident_id}` | "
                        f"Run `{incident.run_id}` | "
                        f"Created {incident.created_at.isoformat()}"
                    ),
                }
            ],
        }
    )

    return blocks


def send_incident_to_slack(
    incident: Incident,
    webhook_url: str,
) -> bool:
    """Send an incident notification to Slack via an incoming webhook.

    Only sends for incidents with **high** or **critical** severity (as
    determined by the reasoning report).  Lower-severity incidents are
    silently skipped (returns ``False``).

    Parameters
    ----------
    incident:
        The incident to notify about.
    webhook_url:
        The Slack incoming-webhook URL.  If empty or ``None``, the
        function logs a warning and returns ``False``.

    Returns
    -------
    bool
        ``True`` if the message was accepted by Slack (HTTP 200),
        ``False`` otherwise (including skipped, missing URL, or errors).
    """
    # Guard: missing / empty webhook URL.
    if not webhook_url:
        logger.warning(
            "Slack webhook URL is not configured; skipping notification "
            "for incident '%s'.",
            incident.incident_id,
        )
        return False

    # Guard: only notify for high/critical severity.
    report_severity: Optional[SeverityLevel] = (
        incident.report.severity if incident.report else None
    )
    if report_severity not in _NOTIFY_SEVERITIES:
        logger.debug(
            "Incident '%s' severity '%s' does not meet Slack notification "
            "threshold; skipping.",
            incident.incident_id,
            report_severity.value if report_severity else "none",
        )
        return False

    # Build and send the payload.
    payload = {
        "text": (
            f"Sentinel Incident [{report_severity.value.upper()}]: "
            f"{incident.dataset}/{incident.stage}"
        ),
        "blocks": _build_blocks(incident),
    }

    try:
        response = requests.post(
            webhook_url,
            json=payload,
            timeout=10,
        )
        if response.status_code == 200:
            logger.info(
                "Slack notification sent for incident '%s'.",
                incident.incident_id,
            )
            return True
        else:
            logger.error(
                "Slack webhook returned HTTP %d for incident '%s': %s",
                response.status_code,
                incident.incident_id,
                response.text[:200],
            )
            return False
    except requests.RequestException as exc:
        logger.error(
            "Failed to send Slack notification for incident '%s': %s",
            incident.incident_id,
            exc,
        )
        return False
