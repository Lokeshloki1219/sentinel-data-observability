# Sentinel — Design Write-up

*An LLM-powered agentic data-pipeline observability system.*

---

## 1. The problem

Data pipelines fail **silently**. A schema change upstream, a dropped batch, a spike in
null values, or a distribution shift can corrupt every downstream dashboard and model —
and the job that produced the bad data often exits cleanly, so traditional infrastructure
monitoring (Airflow alerts, Datadog, PagerDuty) never fires. Hand-writing data-quality
rules for every column doesn't scale, and purely reactive alerting still leaves a human to
diagnose and fix everything from scratch, every time.

Sentinel learns the normal behaviour of each pipeline run and watches **two signal streams
together** — the *data* itself and the pipeline's *operational state* (job status,
duration, retries) — detects statistical anomalies without hand-written rules, and uses an
LLM to **correlate symptoms with causes** into a structured incident report. For a small
set of safe, reversible actions it can carry out the fix **once a human approves**, then
records the outcome to improve future responses.

> **Not an AI code assistant.** Tools like Cursor/Copilot help you *write code in an
> editor*. Sentinel watches *running data* and reacts to *live data problems*. Different
> problem, different product.

The differentiator is the **correlation**: connecting a *data* symptom ("`amount` nulls
spiked in the transactions feed") to its *operational* cause ("upstream job `enrich_balances`
failed and was skipped").

---

## 2. Judgement: the six-layer architecture, built at deliberate fidelity

Sentinel is organized around a six-layer reference architecture for agentic data systems.
The maturity signal isn't building all six at maximum effort — it's being explicit about
*which layers are full-fidelity, which are intentionally light, and which are deliberately
constrained, and why.*

| # | Layer | Fidelity | Why |
|---|-------|----------|-----|
| 1 | Intent (`intent/`) | Light config | Per-dataset YAML SLAs; cheap, high-signal input to severity + thresholds. |
| 2 | Observability (`observability/`) | **Full — core** | Two-stream metrics + the rules-plus-statistics detection engine. The bedrock. |
| 3 | Reasoning (`reasoning/`) | **Full — core** | LLM root-cause correlation; the "intelligent" part. |
| 4 | Action (`action/`) | **Constrained** | Only 2 safe, reversible, gated actions. The industry doesn't yet trust autonomous remediation — neither do we. |
| 5 | Memory (`memory/`) | **Full — differentiator** | RAG over incident history; turns a stateless LLM wrapper into a system that demonstrably improves. |
| 6 | Governance (`governance/`) | Light–medium | Risk-tier gate, reason-coded resolution, suppression, append-only audit, auto-resolution. |

See [architecture.md](architecture.md) for the diagram.

---

## 3. How it works (one cycle)

`orchestrator.py::process_run` implements the Spec §10 control loop for one completed run:

1. **Observe** — for each stage, compute `RunMetrics`; collect `OperationalSignals`.
2. **Detect (before persist)** — run all five checks against the rolling baseline, *then*
   save the current run's metrics. Running detection before persisting is what keeps the
   current run out of its own baseline (so schema diff compares against the true previous
   run, and statistical baselines aren't self-contaminated).
3. **Debounce** — high/critical escalate immediately; low/medium must persist for
   `DEBOUNCE_RUNS` (2) consecutive runs, tracked in the `anomaly_streaks` table.
4. **Suppress** — drop anomalies matching active `SuppressionRule`s (or raise their
   threshold).
5. **Group → Reason** — `group_related` bundles anomalies per `(dataset, stage, run)`;
   for each group, retrieve top-k similar past incidents from Memory, assemble the
   `ReasoningContext`, and call Claude for a schema-valid `ReasoningOutput`. Invalid LLM
   output after one retry → `report_invalid` + rules-only severity.
6. **Persist + Audit** — create the `Incident`, write `incident_created` /
   `report_generated` audit entries.
7. **Gate + Route** — `evaluate_gate` maps risk tier × criticality to a gate; high/critical
   incidents route to Slack; everything surfaces in the dashboard with an action preview.
8. **Auto-resolve** — subsequent runs are watched; if the offending metric returns to
   baseline within `AUTO_RESOLVE_K` (3) runs, the incident is closed with an `Outcome` and
   a `MemoryRecord` is written.

Every resolution path — approve, modify, reject-with-reason, auto-resolve — writes back to
Memory. *Rejections are signal, not noise.*

---

## 4. Evidence: evaluation results

From `data/eval_results.json` (10 clean runs + 6 labelled fault scenarios, no-LLM mode):

| Dimension | Metric | Result |
|-----------|--------|--------|
| Detection | Precision / Recall / F1 vs. injected faults | **1.00 / 1.00 / 1.00** (6/6 fault types) |
| Detection | True / false / missed | 6 TP, **0 FP**, 0 FN |
| Robustness | False-positive rate on clean runs | **0.0** over 10 runs |
| Learning | False-positive trend | Flat at 0 (no FPs to suppress) |
| Attribution | `caused_by` correct on operational-cause faults | *gated behind `--use-llm`* |
| Report quality | Rubric (root cause / action / severity) | *gated behind `--use-llm`* |
| Memory effect | Diagnosis quality with vs. without memory | *gated behind `--use-llm`* |

All six fault types — row-drop, column-null, schema-change, distribution-shift, stale-data,
and operational-cause — are detected. The LLM-dependent metrics (attribution, report
quality, memory ablation) are wired and run with `python -m evaluation.run_experiments
--use-llm` once an `ANTHROPIC_API_KEY` is set.

Detection latency: high/critical anomalies escalate **within 1 run**; low/medium escalate
after 2 consecutive runs by design (debounce).

---

## 5. Documented failure cases

Honest failure cases read as more mature than a claim of perfection. These were found
during build and evaluation:

### 5.1 Heavy-tailed numeric drift → clean-run false positives *(fixed)*
The first distribution-drift implementation took a **z-score of the column mean** against a
short history. On the log-normal balance columns (`oldbalanceOrg`, `newbalanceOrig`) the
batch *mean* swings run-to-run, and with only 2–3 history points the z-score crossed the
threshold on perfectly clean runs — producing high-severity false positives that
immediately escalated.
**Fix:** switch numeric drift to the **median (p50)**, which is stable for heavy-tailed
distributions, and require a real baseline (`config.MIN_BASELINE = 5`) before any
statistical check fires. Clean-run FP rate dropped to 0.
*Where:* `observability/detection/rules.py::check_distribution`.

### 5.2 Zero-variance baseline blind spot *(fixed)*
The synthetic generator produces exactly the same row count every clean run, so the
row-count history had **zero variance**. The original `compute_zscore` returned `0.0`
whenever σ = 0 — which meant a 60% row drop sitting *within* the configured volume bounds
was silently missed.
**Fix:** when the history is constant but the current value differs, that's a *maximal*
deviation, not zero — return a large finite sentinel (`_DEGENERATE_Z = 100.0`, finite so it
stays JSON-serialisable). The row-drop is now caught via the z-score path.
*Where:* `observability/detection/statistical.py::compute_zscore`.

### 5.3 Per-stage incident duplication *(known trade-off)*
`group_related` groups anomalies per `(dataset, stage, run)`. Dataset-level signals like
freshness are identical across all four stages, so a single stale-data fault raises ~4
near-identical incidents in one run. This is faithful to the spec's per-stage incident
model but is noisier than necessary. A future improvement would dedupe dataset-level checks
(freshness, volume) to one incident per run while keeping stage-level checks (schema, null,
drift) per stage.

---

## 6. Design trade-offs

- **Plain orchestrator vs. Prefect/dbt.** The spec permits a plain scheduler. Orchestration
  is *incidental* to the agentic loop, so `orchestrator.py` + `pipeline/flows.py` run the
  pipeline directly — effort went into detection, reasoning, and memory instead. Prefect/dbt
  are CV keywords, not differentiators here.
- **`MIN_BASELINE` vs. detection latency.** Requiring 5 baseline runs before statistical
  checks fire trades a little cold-start latency for far fewer false positives. Rule-based
  checks (freshness, schema, explicit null thresholds, volume bounds) still fire immediately.
- **Median vs. mean for drift.** Median is robust to the heavy tails in financial data; the
  cost is slightly lower sensitivity to changes that move the tail but not the centre. The
  distribution-shift fault (×10 on `amount`) moves the median decisively, so this is the
  right call for this dataset.
- **`suppress` vs. `raise_threshold`.** Suppression rules can either drop a matching anomaly
  outright or raise the deviation threshold it must exceed — both are honoured before
  escalation, so `not_a_problem` feedback can be soft (raise the bar) or hard (silence).

---

## 7. Reproducibility

- Fault injection is **seeded** (`FaultSpec.seed`); the synthetic generator is deterministic
  per `day`.
- Intent/config is versioned (`intent/datasets/transactions.yaml`, `config.py`).
- Every state transition is an append-only `AuditEntry`.
- Re-run end-to-end:
  ```bash
  python scripts/seed_demo.py            # populate the live dashboard DB
  streamlit run dashboard/app.py         # health, incidents, approve/reject/modify
  python -m evaluation.run_experiments   # detection F1 + FP trend (no key needed)
  pytest -q                              # unit + integration tests
  ```
  Add `ANTHROPIC_API_KEY` to `.env` and pass `--use-llm` to light up attribution, report
  quality, and the memory-ablation result.
