# Sentinel — Technical Specification (Build Brief)

**Document type:** Self-contained technical specification.
**Intended reader:** An AI model or engineer that must understand, analyze, or build this system without access to any prior conversation.
**Companion document:** A separate human-oriented build guide exists; this spec is the authoritative source for scope, data contracts, and acceptance criteria. Where they differ, this spec wins.

> **How to use this document.** Sections 1–5 define *what* the system is. Sections 6–12 define *how* it is built (schemas, component contracts, control flow, algorithms). Sections 13–17 define *how to verify* it (build milestones, evaluation, acceptance criteria). All data structures are normative: implementations must conform to the field names and types in Section 7.

---

## 1. System summary

Sentinel is an **agentic data-pipeline observability system**. It monitors a running data pipeline across two signal streams — the **data** produced by each run and the **operational state** of the pipeline jobs — detects anomalies without hand-written rules, uses an LLM to **correlate symptoms with causes** and produce a structured incident report, and (for a small set of safe, reversible actions) executes a fix **only after explicit human approval**. Every incident, decision, and outcome is stored and retrieved to improve future diagnoses.

**Operating loop:** `Observe → Reason → Propose → Approve → Act → Remember`, with continuous watching of subsequent runs for auto-resolution.

**Design stance:** suggest-first; no autonomous, irreversible changes to production data or schema. Human-in-the-loop at the action gate.

---

## 2. Glossary

| Term | Definition |
|---|---|
| **Run** | One execution of the pipeline (or a stage) producing a batch of data and operational signals. |
| **Batch** | The data produced by one run of a dataset/stage. |
| **Data signal** | A measured property of a batch (row count, freshness, null rate, schema, distribution). |
| **Operational signal** | A measured property of the job that produced the batch (status, duration, retries, exit code). |
| **Anomaly** | A single metric deviating from its learned baseline or a declared expectation. |
| **Incident** | A persistent record created from one or more anomalies; carries the report, resolution, and outcome. |
| **Intent** | A declared, per-dataset statement of expectations/SLAs used to prioritize and threshold. |
| **Resolution** | The human decision on an incident (approve / modify / reject-with-reason / snooze). |
| **Suppression rule** | A rule that prevents a known-benign anomaly pattern from re-firing. |
| **Outcome** | Whether and how an incident was resolved, and whether the metric returned to baseline. |

---

## 3. Scope

**In scope**
1. A scheduled data pipeline ingesting/transforming a public dataset (see Section 16).
2. Per-dataset Intent configuration.
3. Observability over two streams: data metrics + operational signals.
4. A rules-plus-statistics anomaly detection engine with a debounce.
5. A synthetic fault-injection harness producing labelled anomalies and operational-cause scenarios.
6. A Memory layer: incident storage + embedding-based retrieval.
7. An LLM Reasoning engine producing a structured, memory- and operationally-augmented incident report that correlates data symptoms with operational causes.
8. A constrained Action layer (2 reversible actions) behind a risk-tiered, reason-coded approval gate.
9. A Governance layer: risk policy, suppression rules, append-only audit log, auto-resolution detection.
10. Routing (Slack/webhook) + a dashboard with approve/reject/modify controls.
11. Evaluation of detection, report quality, root-cause attribution, action success, memory effect, and false-positive trend.

**Out of scope (non-goals)**
- Autonomous, unattended actions.
- Irreversible production changes (in-place data edits, schema alterations).
- Re-implementing infrastructure crash alerting (delegated to existing tools; operational signals are consumed as *context*).
- Multi-tenant operation; full cross-system lineage graphs.

---

## 4. Architecture: six layers

| # | Layer | Responsibility | Build fidelity |
|---|---|---|---|
| 1 | Intent | Declare per-dataset SLAs/expectations; drive prioritization + thresholds | Light (config) |
| 2 | Observability | Collect data metrics + operational signals; detect anomalies | Full (core) |
| 3 | Reasoning | Correlate signals; root-cause; propose fix; assign severity | Full (core) |
| 4 | Action | Execute safe, reversible, gated actions | Constrained |
| 5 | Memory | Store + retrieve past incidents/decisions/outcomes | Full (differentiator) |
| 6 | Governance | Risk policy, approval gate, suppression, audit, auto-resolution | Light–medium |

---

## 5. Data flow

```
INTENT (config) ─┐
                 ▼
PIPELINE run ──▶ OBSERVABILITY
  • data metrics  ──┐
  • operational     ├─▶ DETECTION ──▶ Anomaly(s) ──▶ debounce ──▶ REASONING
    signals       ──┘                                              │
                                         MEMORY (retrieve) ◀───────┤ assemble context
                                                                   ▼
                                                       ReasoningOutput (report)
                                                                   ▼
                                              Incident ──▶ GOVERNANCE (gate by risk tier)
                                                                   ▼
                                  human: approve / modify / reject(reason) / snooze
                                       │approve                    │reject/manual
                                       ▼                           ▼
                                 ACTION (execute) ───▶ OUTCOME   suppression / memory
                                       │                           │
                                       └────▶ watch next runs ─────┴─▶ MEMORY (write)
```

---

## 6. Tech stack & environment

| Concern | Choice | Notes |
|---|---|---|
| Language | Python ≥ 3.11 | |
| Orchestration | Prefect (or Airflow) | Source of operational signals. A plain scheduler is acceptable if time-constrained. |
| Transformation | dbt (optional) or Python | |
| Warehouse | DuckDB (local) | Alt: Postgres. |
| Data-quality checks | Custom Python (scipy/statsmodels); optional Soda Core / Great Expectations | |
| Anomaly stats | scipy, statsmodels; optional Prophet | |
| LLM | Anthropic Claude API, model `claude-sonnet-4-6` | JSON-only structured output. |
| Embeddings + vector store | any embedding model + Chroma / FAISS / pgvector | For Memory retrieval. |
| Records / audit store | SQLite or Postgres | |
| Dashboard | Streamlit | Approve/Reject/Modify UI. |
| Routing | Slack incoming webhook / SMTP | |
| Tooling | git, uv/poetry, ruff, pytest | |

**Environment variables:** `ANTHROPIC_API_KEY`, `SLACK_WEBHOOK_URL`, `SENTINEL_DB_URL` (optional), `SENTINEL_ENV` (`dev`/`eval`).

---

## 7. Data model (normative schemas)

Notation: `field: type — description`. Types are JSON-ish; persist as the implementation prefers but preserve names/semantics.

### 7.1 IntentConfig — `intent/datasets/<dataset>.yaml`
```
dataset: str
owner: str
consumers: [str]
criticality: enum(low|medium|high|critical)
expected_schedule_cron: str
freshness_sla_minutes: int
key_columns: [str]
accepted_null_pct: { <column>: float }     # 0..1, per column tolerance
expected_volume: { min_rows: int, max_rows: int }   # optional
```

### 7.2 RunMetrics (data stream) — one per (run, stage)
```
run_id: str
dataset: str
stage: str
ts_run: datetime
event_time_max: datetime           # latest business timestamp in the batch
row_count: int
freshness_minutes: float           # now - event_time_max
schema_hash: str                   # stable hash of ordered [(name,dtype)]
schema: [ { name: str, dtype: str } ]
null_rate: { <column>: float }     # 0..1
numeric_stats: { <column>: { mean, std, p05, p50, p95, min, max } }
categorical_dist: { <column>: { <value>: float } }   # low-cardinality only
```

### 7.3 OperationalSignals (ops stream) — one per (run, job)
```
run_id: str
job_name: str
status: enum(success|failed|skipped|running|retrying)
started_at: datetime
ended_at: datetime | null
duration_seconds: float | null
retries: int
exit_code: int | null
upstream_jobs: [str]               # declared lineage
```

### 7.4 Anomaly — emitted by the detection engine
```
anomaly_id: str
run_id: str
dataset: str
stage: str
metric: str                        # e.g. "null_rate.amount", "volume.row_count"
check_type: enum(freshness|volume|schema|null_rate|distribution)
observed: number | str
expected: number | str | { min, max }
deviation: float                   # z-score, PSI, or 1/0 for categorical checks
severity_hint: enum(low|medium|high|critical)
detected_at: datetime
escalated: bool                    # passed debounce (Section 11)
```

### 7.5 ReasoningContext — assembled input to the LLM
```
anomaly: Anomaly
intent: IntentConfig
recent_metrics: [RunMetrics]       # last N runs for the same dataset/stage
operational: [OperationalSignals]  # signals for this run + upstream jobs
schema_current: [ {name,dtype} ]
code_version: str                  # git SHA of pipeline at this run
similar_incidents: [ MemoryRecord ]  # top-k retrieved from Memory
```

### 7.6 ReasoningOutput — strict LLM JSON contract
```
{
  "severity": "low|medium|high|critical",
  "likely_root_cause": "string",
  "caused_by": "data_source|upstream_job|schema_change|pipeline_logic|unknown",
  "evidence": ["string", ...],
  "suggested_action": {
     "type": "rerun_job|quarantine_batch|backfill|none|manual",
     "target": "string",            // e.g. job name or batch id; "" if n/a
     "rationale": "string"
  },
  "confidence": 0.0                  // float 0..1
}
```
Rules: the model returns **only** this JSON object — no prose, no markdown fences. Parsers must strip stray fences defensively and validate against this schema; on validation failure, mark the incident `report_invalid` and fall back to a rules-only severity.

### 7.7 Incident — persistent entity
```
incident_id: str
created_at: datetime
dataset: str
stage: str
run_id: str
anomalies: [Anomaly]               # one or more grouped anomalies
context_used: ReasoningContext     # snapshot for reproducibility
report: ReasoningOutput | null
status: enum(open|awaiting_approval|acted|resolved|suppressed|acknowledged_manual|snoozed|rejected|report_invalid)
resolution: Resolution | null
outcome: Outcome | null
embedding_id: str | null
```

### 7.8 ActionDefinition — entry in the action registry
```
action_type: enum(rerun_job|quarantine_batch|backfill)   # extensible
risk_tier: enum(safe|medium|risky|blocked)
reversible: bool
gate: enum(one_click|typed_confirmation|blocked)
preview: fn(target) -> PreviewResult     # dry-run / predicted effect
execute: fn(target) -> ExecResult        # runs ONLY post-approval
undo: fn(target) -> ExecResult | null
```
Default registry (Section 12): `rerun_job` = safe/reversible/one_click; `quarantine_batch` = safe/reversible/one_click; `backfill` = medium/typed_confirmation; `patch_data` and `alter_schema` = blocked.

### 7.9 Resolution — reason-coded human decision
```
incident_id: str
decision: enum(approved|modified|rejected|snoozed)
reason: enum(none|not_a_problem|will_fix_manually|wrong_diagnosis|defer)
modified_action: ActionDefinition | null
manual_fix_note: str | null
decided_by: str
decided_at: datetime
```
Routing by `reason`:
- `not_a_problem` → create SuppressionRule and/or loosen IntentConfig; mark `suppressed`.
- `will_fix_manually` → store `manual_fix_note` to Memory; mark `acknowledged_manual`.
- `wrong_diagnosis` → keep incident open; record negative signal for retrieval.
- `defer` → mark `snoozed`; re-surface later.

### 7.10 Outcome
```
incident_id: str
resolved: bool
resolved_at: datetime | null
time_to_resolution_minutes: float | null
resolution_method: enum(action|manual|auto|none)
fix_worked: bool | null            # did the metric return to baseline within K runs?
```

### 7.11 SuppressionRule
```
rule_id: str
dataset: str
match: { metric: str, check_type: str, condition: str }   # e.g. "step % 24 == 0"
effect: enum(suppress|raise_threshold)
param: float | null                # new threshold if raise_threshold
created_from_incident: str
created_at: datetime
```

### 7.12 MemoryRecord
```
incident_id: str
dataset: str
check_type: str
summary_text: str                  # embedded text (anomaly + cause + fix)
embedding: vector
report: ReasoningOutput
outcome: Outcome
```

### 7.13 AuditEntry — append-only
```
entry_id: str
ts: datetime
incident_id: str | null
event: enum(anomaly_detected|incident_created|report_generated|gate_evaluated|
            action_proposed|resolution_recorded|action_executed|action_undone|
            outcome_recorded|suppression_created)
actor: enum(system|human)
detail: object                     # event-specific payload
```

---

## 8. Component specifications

Each component lists **Inputs → Outputs** and key responsibilities. Module paths match Section 13.

- **intent/** — Loads `IntentConfig` per dataset. Inputs: YAML files. Outputs: validated `IntentConfig`. Provides thresholds/criticality to detection and reasoning.
- **pipeline/ingest.py, transform/** — Runs the data pipeline producing batches. Inputs: source dataset. Outputs: stored tables in the warehouse.
- **pipeline/faults.py** — Fault-injection harness. Inputs: a clean batch + a fault spec. Outputs: a corrupted batch + a ground-truth label `{fault_type, target, params, caused_by}`. Supports injecting an *operational* cause (mark an upstream job `failed/skipped`) that produces a downstream data fault.
- **observability/store.py** — Persists/reads `RunMetrics`, `OperationalSignals`, incidents, audit. 
- **observability/metrics.py** — Computes `RunMetrics` from a batch. Inputs: batch + schema. Outputs: `RunMetrics`.
- **observability/operational.py** — Collects `OperationalSignals` from the orchestrator (or a stub). Outputs: `OperationalSignals`.
- **observability/detection/** — Runs all checks against baselines (Section 11). Inputs: latest `RunMetrics` + history + `IntentConfig` + active `SuppressionRule`s. Outputs: `[Anomaly]` (escalated only).
- **memory/embed.py, retrieve.py, store.py** — Embeds incident summaries; retrieves top-k similar `MemoryRecord`s for a new anomaly. Inputs: anomaly/summary. Outputs: `[MemoryRecord]`.
- **reasoning/context.py** — Assembles `ReasoningContext`. Inputs: anomaly + all sources. Outputs: `ReasoningContext`.
- **reasoning/prompts.py** — Holds the system prompt + report prompt (Section 9).
- **reasoning/reporter.py** — Calls the LLM, validates JSON, persists `Incident.report`. Outputs: `ReasoningOutput`.
- **action/registry.py** — Declares `ActionDefinition`s and risk tiers.
- **action/preview.py** — Produces a `PreviewResult` (dry-run/predicted effect) for a proposed action.
- **action/executor.py** — Executes an approved action; supports undo. Must be sandboxed to the orchestrator + warehouse only.
- **governance/policy.py** — Maps `risk_tier → gate`. Inputs: action + criticality. Outputs: gate decision.
- **governance/approval.py** — Records `Resolution`; routes by `reason` (Section 7.9).
- **governance/suppression.py** — Creates/applies `SuppressionRule`s; loosens Intent expectations.
- **governance/resolution.py** — Auto-resolution detector: watches subsequent runs; sets `Outcome.fix_worked` if the metric returns to baseline within K runs.
- **governance/audit.py** — Appends `AuditEntry`; writes outcomes back to Memory.
- **routing/slack.py** — Sends incident reports for high/critical severity.
- **dashboard/app.py** — Streamlit UI: health timeline, metric charts with anomaly markers, incident feed, and Approve/Reject(reason)/Modify controls with action preview.

---

## 9. Reasoning prompt contract

**System prompt requirements (reasoning/prompts.py):**
- Role: a data-reliability engineer that diagnoses pipeline data anomalies.
- Instruction to correlate the data anomaly with the provided operational signals and similar past incidents.
- Must weigh recent schema/code changes and upstream job failures as likely causes.
- Output: exactly one JSON object matching ReasoningOutput (Section 7.6). No preamble, no markdown.

**User message:** a serialized `ReasoningContext` (Section 7.5). Do **not** include raw row-level data — only metrics, schema, operational signals, intent, and retrieved summaries.

**Output handling:** strip fences → `json.loads` → schema-validate → persist. On failure: retry once with a stricter instruction; if still invalid, set `status=report_invalid` and use rules-only severity (`severity_hint`).

**Cost control:** only invoke the LLM for **escalated, non-suppressed** anomalies, deduplicated per (dataset, metric, run).

---

## 10. Control flow (one cycle)

```
on pipeline_run_complete(run):
    metrics = metrics.compute(run.batch)              # RunMetrics
    ops     = operational.collect(run)                # OperationalSignals
    store.save(metrics); store.save(ops)

    anomalies = detection.run(metrics, history, intent, suppression_rules)
    anomalies = debounce(anomalies, history)          # Section 11
    anomalies = drop_suppressed(anomalies, suppression_rules)
    if not anomalies: return

    for group in group_related(anomalies):
        similar = memory.retrieve(group)              # top-k MemoryRecord
        ctx     = context.assemble(group, intent, history, ops, similar, code_sha)
        report  = reporter.run(ctx)                   # ReasoningOutput (validated)
        incident = incident_create(group, ctx, report)
        audit.append("incident_created", incident)

        action = report.suggested_action
        gate   = policy.gate_for(action, intent.criticality)
        if gate == blocked or action.type in {none, manual}:
            route_report_only(incident)               # no action proposed
        else:
            incident.status = awaiting_approval
            present_for_approval(incident, preview(action))   # dashboard + Slack

# on human decision:
on_resolution(incident, resolution):
    audit.append("resolution_recorded", resolution)
    route_by_reason(resolution)                       # Section 7.9
    if resolution.decision in {approved, modified}:
        result = executor.execute(resolution.effective_action())
        audit.append("action_executed", result)

# continuous:
on_each_subsequent_run:
    resolution_detector.check_open_incidents()        # sets Outcome, writes Memory
```

---

## 11. Detection algorithms (normative)

For each metric, compare the latest `RunMetrics` to a rolling baseline over the last `N` runs (default `N=30`).

- **Freshness:** anomaly if `freshness_minutes > intent.freshness_sla_minutes`. Severity scales with the overage ratio.
- **Volume:** rolling z-score of `row_count`; anomaly if `|z| ≥ 3` OR `row_count` outside `intent.expected_volume`. `deviation = z`.
- **Null-rate (per key column):** anomaly if `null_rate[col] > intent.accepted_null_pct[col]` OR rolling `|z| ≥ 3`.
- **Schema:** anomaly if `schema_hash` differs from the previous run; classify as added/removed/retyped by diffing `schema`. `check_type=schema`, `deviation=1`.
- **Distribution drift (per key numeric/categorical column):** compute **PSI** (or KS) between current and baseline. Anomaly if `PSI ≥ 0.2` (or KS p `< 0.01`). `deviation = PSI`.
- **Severity hint:** derive from deviation magnitude × `intent.criticality` (define a simple lookup table; e.g. schema change on a `critical` dataset → `high`/`critical`).

**Debounce:** anomalies with `severity_hint ∈ {low, medium}` must persist for **2 consecutive runs** before `escalated=true`. `high`/`critical` escalate immediately. Transient single-run blips are recorded but not escalated.

---

## 12. Action & governance (normative)

**Action registry (default):**

| action_type | risk_tier | reversible | gate |
|---|---|---|---|
| rerun_job | safe | yes | one_click |
| quarantine_batch | safe | yes (undo = un-quarantine) | one_click |
| backfill | medium | mostly | typed_confirmation |
| patch_data | risky | no | blocked |
| alter_schema | risky | no | blocked |

**Quarantine semantics:** move the flagged batch rows to a `quarantine` table; `undo` moves them back. Executor touches only the orchestrator (re-trigger) and warehouse (quarantine table). No production data edits.

**Gate logic (policy.py):** `gate = registry[action].gate`, optionally escalated by `intent.criticality` (e.g., a `medium` action on a `critical` dataset → `typed_confirmation`). `blocked` actions are never executed by Sentinel.

**Suppression:** a `not_a_problem` resolution creates a `SuppressionRule`; detection consults active rules and drops/raises thresholds for matching anomalies before escalation.

**Auto-resolution (resolution.py):** for each open incident, watch the next `K` runs (default `K=3`). If the offending metric returns within baseline, set `Outcome.resolved=true`, `resolution_method` (`action`/`manual`/`auto`), `fix_worked`, and `time_to_resolution_minutes`; write a `MemoryRecord`.

**Audit:** every state transition and action is an append-only `AuditEntry`.

---

## 13. Project structure

```
sentinel/
├── README.md
├── pyproject.toml
├── .env.example                 # ANTHROPIC_API_KEY, SLACK_WEBHOOK_URL, SENTINEL_DB_URL
├── docs/ (architecture.png, writeup.md)
├── data/                        # warehouse file + raw batches (gitignored)
│
├── intent/
│   └── datasets/<dataset>.yaml  # (1) IntentConfig
│
├── pipeline/
│   ├── flows.py                 # Prefect flows
│   ├── ingest.py
│   ├── transform/               # dbt project or transform.py
│   └── faults.py                # fault-injection harness (+ operational-cause faults)
│
├── observability/               # (2)
│   ├── store.py
│   ├── metrics.py               # data signals -> RunMetrics
│   ├── operational.py           # ops signals -> OperationalSignals
│   └── detection/
│       ├── engine.py
│       ├── rules.py             # freshness/schema/threshold
│       └── statistical.py       # z-score, IQR, PSI/KS, debounce
│
├── memory/                      # (5)
│   ├── store.py
│   ├── embed.py
│   └── retrieve.py
│
├── reasoning/                   # (3)
│   ├── context.py               # assemble ReasoningContext
│   ├── prompts.py
│   └── reporter.py              # LLM call + JSON validation
│
├── action/                      # (4)
│   ├── registry.py
│   ├── preview.py
│   └── executor.py              # post-approval only; sandboxed
│
├── governance/                  # (6)
│   ├── policy.py
│   ├── approval.py              # reason-coded resolutions
│   ├── suppression.py
│   ├── resolution.py            # auto-resolution detection
│   └── audit.py
│
├── routing/slack.py
├── dashboard/app.py             # Streamlit (approve/reject/modify + preview)
├── evaluation/
│   ├── run_experiments.py
│   ├── detection_metrics.py
│   ├── attribution.py           # data symptom -> infra cause
│   ├── report_rubric.py
│   └── memory_ablation.py
└── tests/test_*.py
```

---

## 14. Build milestones (each independently verifiable)

| Milestone | Delivers | Done when |
|---|---|---|
| **M1 — Observe** | Pipeline + Observability (both streams) + dashboard | A scheduled run stores `RunMetrics` + `OperationalSignals`; dashboard shows health timeline; detection emits `Anomaly` objects on injected faults. |
| **M2 — Reason** | Reasoning engine + reports | For an escalated anomaly, a schema-valid `ReasoningOutput` is produced, persisted as an `Incident`, and routed to Slack/dashboard. |
| **M3 — Remember** | Memory layer + retrieval | New incidents retrieve top-k similar prior incidents; the with-vs-without-memory evaluation (Section 15) runs and reports a number. |
| **M4 — Act & Govern** | Action registry + risk-tiered, reason-coded approval gate + suppression + auto-resolution + audit | An approved `rerun_job`/`quarantine_batch` executes (with undo), every transition is audited, rejections route by reason, and auto-resolution closes incidents whose metric returns to baseline. |

---

## 15. Evaluation protocol

**Setup:** generate clean batches + inject labelled faults via `faults.py`. Fault types: row-drop, column-null, distribution-shift, schema-change, stale-data, and **operational-cause** (upstream job marked failed/skipped producing a downstream data fault). **Bootstrap Memory first** by replaying many faults so retrieval has a corpus before measuring the memory effect.

| Metric | Definition |
|---|---|
| Detection precision/recall/F1 | vs. injected ground-truth faults |
| Detection latency | runs between fault occurrence and escalation |
| Report quality | rubric (root cause correct? action appropriate? severity reasonable?), sampled; optional LLM-as-judge |
| **Root-cause attribution** | on operational-cause faults, fraction where `caused_by` correctly = `upstream_job` |
| Action success | fraction of approved actions where `Outcome.fix_worked=true` |
| **Memory effect** | report-quality delta, with vs. without retrieved incidents (ablation) |
| **False-positive trend** | FP rate over successive runs as `not_a_problem` suppression accumulates (expect downward) |
| Robustness | FP rate on clean runs |

**Reproducibility:** seed fault injection; version intent/config; log all `AuditEntry`s; record failure cases.

---

## 16. Dataset specification

**Primary: PaySim** (synthetic mobile-money transactions). Columns: `step` (hourly index), `type`, `amount`, `nameOrig`, `oldbalanceOrg`, `newbalanceOrig`, `nameDest`, `oldbalanceDest`, `newbalanceDest`, `isFraud`, `isFlaggedFraud`. Synthetic (no privacy concerns), interpretable columns, a real time axis, high volume.

**Pipeline stages to monitor:** `raw_transactions → cleaned_typed → enriched (balances, account joins) → fraud_scoring_features`. Batch by `step`-derived day. Each stage is a job emitting `OperationalSignals`; each output table is profiled into `RunMetrics`.

**Intent example (`intent/datasets/transactions.yaml`):** criticality `high`; `freshness_sla_minutes` per the batch cadence; `key_columns: [amount, type, oldbalanceOrg, newbalanceOrig]`; `accepted_null_pct` small for balance columns.

**Alternatives:** Lending Club loans (real, ~145 cols, issue-date) for richer schema-drift storytelling; IBM AML transactions (synthetic, timestamped, laundering labels) for an AML framing. **Avoid** PCA-anonymized credit-card fraud data (`V1…V28`) — non-interpretable columns make root-cause narratives meaningless.

---

## 17. Whole-project acceptance criteria

The build is complete when all hold:
1. A scheduled PaySim pipeline runs and persists `RunMetrics` + `OperationalSignals` per run/stage.
2. The detection engine emits `Anomaly` objects for all five data checks plus the debounce, validated against injected faults with reported precision/recall/F1.
3. The reasoning engine returns schema-valid `ReasoningOutput`, correlating data symptoms with operational causes, with a measured root-cause-attribution score on operational-cause faults.
4. Memory retrieval is wired into reasoning and the with-vs-without-memory effect is measured.
5. The action layer executes the two reversible actions only after a risk-tiered, reason-coded approval; rejections route by reason; suppression and auto-resolution function; every transition is audited.
6. A dashboard presents health, incidents, and approve/reject/modify controls with action preview.
7. Evaluation (Section 15) is reported, including the false-positive trend and at least two documented failure cases.

---

*End of specification. All schemas in Section 7 are normative; implementations must preserve field names and semantics for interoperability.*
