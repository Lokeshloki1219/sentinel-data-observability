# Sentinel

**Agentic Data-Pipeline Observability System**

Sentinel monitors a running data pipeline across two signal streams — the **data** produced by each run and the **operational state** of the pipeline jobs — detects anomalies without hand-written rules, uses an LLM to **correlate symptoms with causes** and produce a structured incident report, and (for a small set of safe, reversible actions) executes a fix **only after explicit human approval**.

## Operating Loop

`Observe → Reason → Propose → Approve → Act → Remember`

## Quick Start

```bash
# 1. Create virtual environment
python -m uv venv --python 3.11 .venv
.venv\Scripts\activate

# 2. Install dependencies
python -m uv pip install -e ".[gpu]"

# 3. Configure environment
copy .env.example .env
# Edit .env with your API keys

# 4. Run the pipeline
python -m pipeline.flows

# 5. Launch the dashboard
streamlit run dashboard/app.py
```

## Architecture

| Layer | Module | Purpose |
|-------|--------|---------|
| Intent | `intent/` | Per-dataset SLA configuration |
| Observability | `observability/` | Metrics collection + anomaly detection |
| Reasoning | `reasoning/` | LLM-powered root-cause analysis |
| Memory | `memory/` | Past incident retrieval via embeddings |
| Action | `action/` | Safe, reversible, gated fixes |
| Governance | `governance/` | Risk policy, approval, suppression, audit |

## Evaluation

```bash
python -m evaluation.run_experiments
```
