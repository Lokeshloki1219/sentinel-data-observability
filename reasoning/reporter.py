"""
Sentinel — LLM Report Generator (Section 9).

Calls the Anthropic API with an assembled ``ReasoningContext``,
validates the structured JSON response against ``ReasoningOutput``,
and returns the parsed report.

Cost control note: the *caller* is responsible for ensuring only
escalated, non-suppressed anomalies reach ``Reporter.generate_report``.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional, Tuple

import anthropic
from pydantic import ValidationError

from config import config
from schemas import ReasoningContext, ReasoningOutput
from reasoning.prompts import SYSTEM_PROMPT, RETRY_INSTRUCTION, build_user_message

logger = logging.getLogger(__name__)

# Regex to strip markdown code fences (```json ... ``` or ``` ... ```)
_FENCE_RE = re.compile(
    r"^```(?:json)?\s*\n?(.*?)\n?\s*```$",
    re.DOTALL | re.MULTILINE,
)


def _strip_fences(text: str) -> str:
    """Remove markdown code fences from LLM output, if present."""
    text = text.strip()
    match = _FENCE_RE.search(text)
    if match:
        return match.group(1).strip()
    return text


def _parse_response(raw_text: str) -> ReasoningOutput:
    """Strip fences → json.loads → validate against ReasoningOutput.

    Raises
    ------
    ValueError
        If the text is not valid JSON.
    pydantic.ValidationError
        If the JSON does not match the ``ReasoningOutput`` schema.
    """
    cleaned = _strip_fences(raw_text)
    data = json.loads(cleaned)
    return ReasoningOutput.model_validate(data)


class Reporter:
    """LLM-backed incident report generator.

    Calls the Anthropic messages API to produce a structured
    :class:`ReasoningOutput` from a :class:`ReasoningContext`.
    """

    def __init__(self) -> None:
        """Initialize the Anthropic client using the configured API key."""
        self._client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
        self._model: str = config.ANTHROPIC_MODEL

    def generate_report(
        self, ctx: ReasoningContext
    ) -> Tuple[Optional[ReasoningOutput], bool]:
        """Generate a structured incident report from reasoning context.

        Workflow:
        1. Build the user message from the context.
        2. Call the Anthropic API (``claude-sonnet-4-6``, temperature 0.2).
        3. Parse the response: strip markdown fences → ``json.loads`` →
           validate with ``ReasoningOutput``.
        4. On failure: retry **once** with a stricter instruction appended.
        5. If still invalid: return ``(None, False)``.

        Parameters
        ----------
        ctx:
            The assembled :class:`ReasoningContext`.

        Returns
        -------
        tuple[ReasoningOutput | None, bool]
            ``(report, valid)`` where ``valid`` is ``True`` on success
            and ``False`` when the LLM response could not be parsed
            (indicating ``report_invalid`` status).
        """
        user_message = build_user_message(ctx)

        # ── First attempt ──────────────────────────────────────────────
        report = self._try_call(user_message)
        if report is not None:
            return report, True

        # ── Retry with stricter instruction ────────────────────────────
        logger.warning(
            "First LLM attempt produced invalid output; retrying with "
            "stricter instruction."
        )
        stricter_message = user_message + RETRY_INSTRUCTION
        report = self._try_call(stricter_message)
        if report is not None:
            return report, True

        logger.error(
            "LLM failed to produce valid ReasoningOutput after retry."
        )
        return None, False

    # ── Internal helpers ───────────────────────────────────────────────

    def _try_call(self, user_message: str) -> Optional[ReasoningOutput]:
        """Make a single API call and attempt to parse the result.

        Returns ``None`` if the call fails or the output is invalid.
        """
        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=512,
                temperature=0.2,
                system=SYSTEM_PROMPT,
                messages=[
                    {"role": "user", "content": user_message},
                ],
            )
            raw_text: str = response.content[0].text
            logger.debug("LLM raw response: %s", raw_text[:500])
            return _parse_response(raw_text)

        except (json.JSONDecodeError, ValidationError) as exc:
            logger.warning("Failed to parse LLM response: %s", exc)
            return None
        except anthropic.APIError as exc:
            logger.error("Anthropic API error: %s", exc)
            return None
