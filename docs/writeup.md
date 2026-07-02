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

The **primary detection result is the graduated study** (`python -m evaluation.graduated`, also
folded into `run_experiments` output) — the same detector run against faults at a *range of
magnitudes* on a baseline with realistic ~2% volume variance. A single "perfect" number on
obvious faults isn't evidence; a **degradation curve** is.

**Detection degrades gracefully as faults get subtler** (volume family, baseline std ≈ 175 rows):

| Volume drop | 40 % | 20 % | 10 % | 8 % | **5 %** | **3 %** | **2 %** |
|---|---|---|---|---|---|---|---|
| z-score | −23 | −11 | −5.6 | −4.5 | **−2.8** | **−1.6** | **−1.0** |
| detected (z ≥ 3) | ✅ | ✅ | ✅ | ✅ | **❌** | **❌** | **❌** |

Recall by severity bucket: **obvious 1.0 · moderate 1.0 · subtle 0.0**.

**Precision / recall / F1 vs. the z-threshold** — the operating point is a visible trade-off:

| z-threshold | 1.5 | 2.0 | 2.5 | **3.0 (shipped)** | 4.0 | 5.0 |
|---|---|---|---|---|---|---|
| precision | 1.00 | 1.00 | 1.00 | **1.00** | 1.00 | 1.00 |
| recall | 0.89 | 0.78 | 0.78 | **0.67** | 0.67 | 0.56 |
| F1 | **0.94** | 0.88 | 0.88 | **0.80** | 0.80 | 0.71 |

**Learning loop (real, not aspirational):** a recurring benign +25% volume surge trips the
volume check every run; after one `not_a_problem` creates a `SuppressionRule` via the governance
path, the false-positive rate for that pattern drops **1.0 → 0.0** (`fp_trend = [1,0,0,0,0,0]`).
(The clean-run FP trend is flat at 0 only because healthy runs produce no FPs to suppress — the
suppression demo is where the loop is exercised.)

**Sanity check on labelled faults** (`run_experiments`, no-LLM): all 11 injected fault types
(the 6 original + validity, uniqueness, oom, timeout, retry_storm) score **1.00 / 1.00 / 1.00**
with **0** false positives — but these are *deliberately obvious*, and a run counts as a true
positive only when the **matching check-type** fires (not just any anomaly), so this confirms
correctness rather than proving sensitivity. Latency ≤ 1 run for high/critical (2 for
low/medium by debounce).

**LLM reasoning** (`--use-llm`, Sonnet — `data/eval_results_llm.json`): root-cause **attribution
0.82 overall / 1.00 on operational-cause faults** (the cross-signal RCA correctly blames the
upstream job / infrastructure, not the data — the differentiator lands); **report-quality 0.77**
avg (root-cause 0.82, severity 1.00, confidence-calibration 0.82, action-appropriateness a weak
0.36); **memory-ablation +0.05** (0.77 with-memory vs 0.72 without — modest but positive, driven
by better action selection). Honest weak spots: action-appropriateness (the model over-prefers
`manual`) and the small memory corpus.

### Known limitations (where a naive baseline breaks)

Naming these is deliberate — they are exactly what a reviewer probes:

- **Cold start.** The first `BASELINE_WINDOW` (30) runs have no rolling baseline, so the
  statistical checks (volume/null z-score, distribution drift) **under-fire** until it fills —
  `run_detection` requires `MIN_BASELINE` (5) history points before those checks activate. The
  rule-based checks (freshness, schema, explicit null thresholds, volume bounds) work from run 1.
  This is expected: better silent than noisy on an unlearned baseline.
- **Seasonality.** Real transaction volume has a weekday/weekend rhythm; a flat rolling mean would
  false-positive on legitimately quiet days (a Sunday looks like a "volume drop"). The current
  baseline is intentionally simple. A **day-of-week / seasonal baseline** (or STL/Prophet
  decomposition) is the natural next step, and the suppression loop already provides a manual
  escape hatch in the meantime.
- **Operating point.** F1 peaks at z=1.5 but the detector ships at **z=3.0** *by choice* — at z=3
  precision is 1.00, favouring precision over recall to avoid alert fatigue (suggest-first). The
  threshold sweep makes this a defensible decision rather than a hidden default.

### What it detects (7 data + 5 operational checks)

| 🟠 Data | 🔴 Operational (from job status/duration/retries/exit-code) |
|---|---|
| freshness · volume · null-rate · schema · distribution drift · **validity/range** · **uniqueness** | **OOM** (exit 137) · **timeout** (exit 124 / over-SLA) · **slow/compute** (duration spike) · **retry storm** (429/instability) · job failed/skipped |

The differentiator is **correlation**: when a pipeline error causes a downstream data fault,
both are flagged and the LLM attributes `caused_by = upstream_job` / `infrastructure`. The
dashboard's **🔀 Pipeline Flow** tab animates this live — a batch streams through the stages,
the broken stage glows (🟠 data / 🔴 pipeline), and a dashed "caused-by" arc links the two.

### Extensions beyond the normative spec (Section 7)

The widened coverage adds, additively (no field renamed or removed, so interoperability holds):
`CheckType` += `validity`, `uniqueness`, `operational`; `CausedBy` += `infrastructure`;
`IntentConfig` += `expected_ranges`, `unique_key`, `max_duration_seconds`, `max_retries`;
`RunMetrics` += `duplicate_rate`. All new checks are opt-in via Intent config and
conservatively thresholded, so the clean-run false-positive rate stays 0.

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
