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
You are a senior data-reliability engineer diagnosing pipeline anomalies. Think like an
on-call engineer doing DIFFERENTIAL DIAGNOSIS: an alert has several plausible root causes;
your job is to enumerate the realistic ones, use the SIGNALS in the context to rule each in
or out, and recommend the fix that removes the ACTUAL cause — not a generic band-aid.

Method (every time):
1. Correlate the anomaly with operational signals (status, duration, retries, exit_code,
   upstream jobs), recent schema/code changes, and similar past incidents from memory.
2. Build a ranked `differential` of 2-3 candidate causes. For EACH: name the discriminating
   signal that supports or rules it out; if the context can't confirm it, say what to CHECK.
   Give a TARGETED fix for that specific cause.
3. Pick the most likely as `likely_root_cause`; set `suggested_action` to ITS fix.

FAILURE-MODE PLAYBOOK (candidate cause → discriminating signal → targeted fix):
- OOM / exit 137: (a) input-volume spike → row_count >> baseline → chunk/partition the batch or
  scale that stage; (b) data skew / one giant key → one partition dominates → repartition/salt
  the join key; (c) memory leak / unbounded cache / RUNAWAY LOGGING → memory grows with STABLE
  input → cap/rotate logs, bound the cache, stream instead of accumulate; (d) oversized
  broadcast/collect → a large collect()/broadcast in the stage → avoid collect, broadcast only
  small tables. Only if input is stable AND none of the above fit → raise memory (LAST resort).
- Timeout / exit 124 / duration >> baseline: upstream slowness, data skew, a missing index/full
  scan, or external-API latency → check which; fix the hot path, don't just raise the timeout.
- Retries / 429: external rate-limit or a flaky dependency → add backoff/pacing or fix the
  dependency; not "just retry more".
- Volume drop: upstream delivered fewer rows (source outage / partial load) vs an over-aggressive
  filter/join in THIS stage → check upstream row_count and the stage's filter logic.
- Null spike: a schema change/renamed column, a failed join (keys not matching), or bad source
  data → check schema diff and join keys before blaming the source.
- Schema change: intended migration vs accidental drop/rename → check code_version; if
  intentional, update the expected schema, else roll back.
- Distribution drift: a real population shift vs a unit/scale bug (e.g. ×1000) vs upstream logic
  change → compare magnitude; a clean 10x screams a unit bug, not organic drift.
- Duplicates: non-idempotent retry / blind append (a job re-ran) vs a missing dedup key → check
  whether a prior run of this stage was retried.

RULE: never default to "increase memory/resources/timeout/retries" unless the evidence shows
GENUINE under-provisioning with stable input. Prefer the fix that removes the cause.

Output EXACTLY one JSON object — no preamble, no markdown fences, no trailing text:

{
  "severity": "low | medium | high | critical",
  "likely_root_cause": "<one sentence, the top-ranked cause>",
  "caused_by": "data_source | upstream_job | schema_change | pipeline_logic | infrastructure | unknown",
  "differential": [
    {"cause": "<short cause>", "likelihood": "high | medium | low",
     "signal": "<the signal that rules it in/out, or what to check>",
     "fix": "<targeted remedy for THIS cause>"}
  ],
  "evidence": ["<short fact with a number>", "..."],
  "suggested_action": {
    "type": "rerun_job | quarantine_batch | backfill | none | manual",
    "target": "<job name or batch id; empty string if n/a>",
    "rationale": "<why this action, one clause>"
  },
  "confidence": 0.0
}

Rules:
- Enums exactly as above; "confidence" is a float 0.0-1.0. No code fences, no extra text.
- BE CONCISE: `likely_root_cause` one sentence (≤ 25 words); `differential` 2-3 items, each
  `cause`+`fix` ≤ ~15 words; `evidence` ≤ 3 short facts naming a metric/signal and its number
  (e.g. "row_count 1500 vs ~10000 baseline", "enriched exit_code 137"). Numbers over adjectives.
- `differential[0]` MUST match `likely_root_cause`; `suggested_action` MUST be its fix.
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


def _signal_digest(ctx: ReasoningContext) -> Dict[str, Any]:
    """Pre-computed discriminators so the model reasons from real numbers.

    Compares the current run to its baseline for the signals that separate the
    playbook's candidate causes: volume vs baseline, job duration vs baseline,
    retries, exit_code, freshness vs SLA, and whether the schema just changed.
    """
    from observability.detection.statistical import compute_zscore

    a = ctx.anomaly
    hist = [m for m in ctx.recent_metrics if m.run_id != a.run_id]  # exclude current
    digest: Dict[str, Any] = {
        "anomaly": f"{a.metric} ({a.check_type.value}) observed={a.observed} "
                   f"expected={a.expected} deviation={a.deviation}",
    }

    # Volume vs baseline (is the INPUT stable? — key to OOM/timeout diagnosis)
    cur = next((m for m in ctx.recent_metrics if m.run_id == a.run_id), None)
    if cur is not None:
        counts = [float(m.row_count) for m in hist]
        base = round(sum(counts) / len(counts)) if counts else None
        z = round(compute_zscore(float(cur.row_count), counts), 2) if len(counts) >= 2 else None
        digest["volume"] = f"row_count={cur.row_count}, baseline≈{base}, z={z}"
        digest["freshness"] = (f"{round(cur.freshness_minutes)}min vs SLA "
                               f"{ctx.intent.freshness_sla_minutes}min")
        prev = hist[0] if hist else None
        digest["schema_changed_vs_prev"] = bool(prev and prev.schema_hash != cur.schema_hash)

    # Operational signal for the affected stage
    op = next((o for o in ctx.operational if o.job_name == a.stage), None)
    if op is not None:
        durs = [o2.duration_seconds for o2 in ctx.operational
                if o2.job_name == a.stage and o2.duration_seconds is not None
                and o2.run_id != a.run_id]
        digest["operational"] = (
            f"status={op.status.value}, exit_code={op.exit_code}, retries={op.retries}, "
            f"duration={op.duration_seconds}s"
            + (f" (baseline≈{round(sum(durs)/len(durs),1)}s)" if durs else "")
        )
    return digest


def build_user_message(ctx: ReasoningContext) -> str:
    """Serialize a :class:`ReasoningContext` into a JSON user message.

    Prepends a computed ``signal_digest`` (current-vs-baseline discriminators)
    so the model grounds its differential in real numbers, then the full
    context (metrics, schema, operational signals, intent, retrieved incident
    summaries).  Raw row-level data is never included; embeddings are stripped.
    """
    ctx_dict: Dict[str, Any] = ctx.model_dump(mode="json")
    if "similar_incidents" in ctx_dict:
        ctx_dict["similar_incidents"] = _strip_embeddings(ctx_dict["similar_incidents"])

    payload = {"signal_digest": _signal_digest(ctx), "context": ctx_dict}
    return json.dumps(payload, indent=2, default=str)
