# Sentinel — Architecture

> These diagrams use [Mermaid](https://mermaid.js.org/), which GitHub renders natively.
> To produce the `architecture.png` named in the spec, open this file on GitHub (or in
> VS Code with a Mermaid extension) and export the rendered diagram.

## Six-layer reference architecture + data flow

```mermaid
flowchart TD
    INTENT["(1) INTENT — per-dataset SLAs, owners, expectations<br/>intent/datasets/*.yaml"]

    subgraph PIPE["DATA PIPELINE — pipeline/"]
        ING["ingest.py — synthetic PaySim batch"]
        FAULTS["faults.py — fault-injection harness<br/>(labelled ground truth)"]
        TX["transform/ — raw → cleaned → enriched → fraud_features"]
        ING --> FAULTS --> TX
    end

    subgraph OBS["(2) OBSERVABILITY — observability/"]
        METRICS["metrics.py — RunMetrics (data stream)"]
        OPS["operational.py / flows.py — OperationalSignals (ops stream)"]
        DET["detection/ — freshness · volume · null · schema · drift<br/>+ debounce + suppression"]
        METRICS --> DET
        OPS --> DET
    end

    REASON["(3) REASONING — reasoning/<br/>Claude → ReasoningOutput JSON<br/>{root_cause, caused_by, severity, action}"]
    MEMORY["(5) MEMORY — memory/<br/>ChromaDB: incidents, outcomes, negatives"]
    GOV["(6) GOVERNANCE — governance/<br/>risk-tier gate · suppression · audit · auto-resolve"]
    ACTION["(4) ACTION — action/<br/>rerun_job · quarantine_batch (reversible, sandboxed)"]
    OUT["routing/slack.py + dashboard/app.py<br/>(approve / reject / modify + preview)"]

    INTENT --> OBS
    TX --> METRICS
    DET -->|"escalated Anomaly(s)"| REASON
    MEMORY <-->|"retrieve top-k / write outcome"| REASON
    REASON -->|"Incident + report"| GOV
    GOV -->|"approved ✓"| ACTION
    GOV -->|"rejected ✗ (reason-coded)"| MEMORY
    ACTION -->|"Outcome"| GOV
    GOV --> OUT
    GOV -->|"watch next runs → auto-resolve"| MEMORY

    classDef full fill:#1e3a8a,stroke:#3b82f6,color:#fff;
    classDef light fill:#374151,stroke:#9ca3af,color:#fff;
    classDef constrained fill:#7c2d12,stroke:#f97316,color:#fff;
    class OBS,REASON,MEMORY full;
    class INTENT,GOV light;
    class ACTION constrained;
```

`orchestrator.py::process_run` is the glue that runs this whole cycle for one completed
pipeline run (Spec §10): detect-before-persist → group → retrieve → reason → create
incident → audit → gate → route → auto-resolve.

## The operating loop

```mermaid
flowchart LR
    O["Observe<br/>(2 signal streams + debounce)"] --> R["Reason<br/>(LLM + memory + ops context)"]
    R --> P["Propose<br/>(incident report + suggested action)"]
    P --> A{"Resolve?<br/>(human, gated by risk tier)"}
    A -->|approve / modify| ACT["Act<br/>(reversible action)"]
    A -->|reject (reason-coded)| MEM["Remember<br/>(suppression / manual note / negative signal)"]
    ACT --> MEM
    MEM --> W["Watch next runs"]
    W -->|metric back to baseline| AUTO["Auto-resolve<br/>(Outcome + MemoryRecord)"]
    AUTO --> O
    W --> O
```

## Fidelity at a glance

| # | Layer | Fidelity | Module |
|---|-------|----------|--------|
| 1 | Intent | Light (config) | `intent/` |
| 2 | Observability | **Full (core)** | `observability/` |
| 3 | Reasoning | **Full (core)** | `reasoning/` |
| 4 | Action | Constrained (2 reversible, gated) | `action/` |
| 5 | Memory | **Full (differentiator)** | `memory/` |
| 6 | Governance | Light–medium | `governance/` |
