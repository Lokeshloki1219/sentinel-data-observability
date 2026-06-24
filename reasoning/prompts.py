"""
Sentinel — Reasoning Prompt Templates (Section 9).

Contains the system prompt and the user-message builder for the
LLM reasoning engine.  Output must be exactly one JSON object
matching :class:`ReasoningOutput` — no preamble, no markdown.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List

from schemas import ReasoningContext


# ── System Prompt ──────────────────────────────────────────────────────────

SYSTEM_PROMPT: str = """\
You are a senior data-reliability engineer responsible for diagnosing \
pipeline data anomalies.

When presented with an anomaly, you MUST:
1. Correlate the data anomaly with the provided operational signals \
(job statuses, durations, retries, upstream job results) and similar \
past incidents retrieved from memory.
2. Weigh recent schema changes and code-version changes as likely causes.
3. Weigh upstream job failures or retries as likely causes of downstream \
data faults.
4. Reference concrete evidence from the provided context to justify your \
root-cause conclusion.

Your output MUST be exactly one JSON object — no preamble, no explanation, \
no markdown fences, no trailing text.  The JSON object must have these \
exact fields:

{
  "severity": "low | medium | high | critical",
  "likely_root_cause": "<concise description of the root cause>",
  "caused_by": "data_source | upstream_job | schema_change | pipeline_logic | unknown",
  "evidence": ["<evidence string 1>", "..."],
  "suggested_action": {
    "type": "rerun_job | quarantine_batch | backfill | none | manual",
    "target": "<job name or batch id; empty string if not applicable>",
    "rationale": "<why this action>"
  },
  "confidence": 0.0
}

Rules:
- "severity" is one of: low, medium, high, critical.
- "caused_by" is one of: data_source, upstream_job, schema_change, \
pipeline_logic, unknown.
- "suggested_action.type" is one of: rerun_job, quarantine_batch, \
backfill, none, manual.
- "confidence" is a float between 0.0 and 1.0 inclusive.
- Do NOT wrap the JSON in markdown code fences.
- Do NOT include any text before or after the JSON object.
"""


# ── Stricter retry instruction ─────────────────────────────────────────────

RETRY_INSTRUCTION: str = (
    "\n\nYour previous response was not valid JSON matching the required "
    "schema.  Return ONLY the raw JSON object — no markdown fences, "
    "no explanation, no preamble.  Every field is required."
)


# ── User message builder ──────────────────────────────────────────────────


def _strip_embeddings(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Remove ``embedding`` vectors from serialized MemoryRecords.

    Embeddings are high-dimensional float arrays that consume tokens
    without adding diagnostic value for the LLM.
    """
    cleaned: List[Dict[str, Any]] = []
    for rec in records:
        rec_copy = {k: v for k, v in rec.items() if k != "embedding"}
        cleaned.append(rec_copy)
    return cleaned


def build_user_message(ctx: ReasoningContext) -> str:
    """Serialize a :class:`ReasoningContext` into a JSON user message.

    The serialization includes **only** metrics, schema, operational
    signals, intent configuration, and retrieved incident summaries.
    Raw row-level data is never included.  Embeddings are stripped
    from ``MemoryRecord`` entries to save tokens.

    Parameters
    ----------
    ctx:
        The assembled reasoning context.

    Returns
    -------
    str
        A JSON string suitable for the LLM user message.
    """
    # Serialize the full context to a dict, then prune.
    ctx_dict: Dict[str, Any] = ctx.model_dump(mode="json")

    # Strip embedding vectors from similar_incidents
    if "similar_incidents" in ctx_dict:
        ctx_dict["similar_incidents"] = _strip_embeddings(
            ctx_dict["similar_incidents"]
        )

    return json.dumps(ctx_dict, indent=2, default=str)
