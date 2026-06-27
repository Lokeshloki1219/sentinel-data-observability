"""
Sentinel — Streamlit Dashboard (§13).

Health timeline, incident feed, approve/reject/modify controls
with action preview. Implements the full 4-way resolution routing UI.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path
_root = str(Path(__file__).resolve().parent.parent)
if _root not in sys.path:
    sys.path.insert(0, _root)

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timezone

from config import config
from schemas import (
    ActionType,
    ActorType,
    AuditEvent,
    Criticality,
    DecisionType,
    GateType,
    Incident,
    IncidentStatus,
    MemoryRecord,
    Outcome,
    ReasonCode,
    Resolution,
    ResolutionMethod,
    RunMetrics,
    SeverityLevel,
)
from memory.embed import build_summary_text
from observability.store import SentinelStore
from action.registry import get_action, is_blocked, ACTION_REGISTRY
from action.preview import preview_action
from action.executor import ActionExecutor
from governance.policy import evaluate_gate
from governance.approval import process_resolution
from governance.audit import log_event
from memory.store import MemoryStore
from intent.parser import load_intent
from pipeline.flows import run_pipeline
from pipeline.faults import FaultSpec
from orchestrator import process_run
from dashboard.flow_graph import render_flow
import streamlit.components.v1 as components

# Pipeline lineage (stage order + display labels) for the flow graph.
_STAGE_ORDER = ["raw_transactions", "cleaned_typed", "enriched", "fraud_scoring_features"]
_STAGE_LABELS = {
    "raw_transactions": "raw",
    "cleaned_typed": "cleaned",
    "enriched": "enriched",
    "fraud_scoring_features": "fraud_features",
}
_DATA_CHECKS = {"freshness", "volume", "null_rate", "schema", "distribution",
                "validity", "uniqueness"}


def _op_badge(a) -> str:
    """Human-readable badge for an operational anomaly (reason from metric)."""
    reason = a.metric.split(".")[-1]
    return {
        "oom": "OOM (exit 137)",
        "timeout": "timeout",
        "slow": "slow job",
        "job_failed": "job failed",
        "retry_storm": f"retry ×{a.observed}",
    }.get(reason, reason)


# ── Initialise singletons ────────────────────────────────────────────────

@st.cache_resource
def _get_store() -> SentinelStore:
    return SentinelStore(config.DB_URL)


@st.cache_resource
def _get_memory_store() -> MemoryStore:
    return MemoryStore(str(config.CHROMA_DIR))


# ── Page config ──────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Sentinel — Pipeline Observability",
    page_icon="🛡️",
    layout="wide",
)

SEVERITY_COLORS = {
    "low": "#22c55e",
    "medium": "#f59e0b",
    "high": "#f97316",
    "critical": "#ef4444",
}

STATUS_COLORS = {
    "open": "#3b82f6",
    "awaiting_approval": "#f59e0b",
    "acted": "#8b5cf6",
    "resolved": "#22c55e",
    "suppressed": "#6b7280",
    "acknowledged_manual": "#06b6d4",
    "snoozed": "#a78bfa",
    "rejected": "#ef4444",
    "report_invalid": "#dc2626",
}


def main():
    store = _get_store()
    memory_store = _get_memory_store()
    executor = ActionExecutor(store)

    st.title("🛡️ Sentinel")
    st.caption("Agentic Data-Pipeline Observability")

    tab_flow, tab_health, tab_incidents, tab_audit = st.tabs(
        ["🔀 Pipeline Flow", "📈 Health Timeline", "🚨 Incidents", "📋 Audit Log"]
    )

    with tab_flow:
        _render_pipeline_flow(store, memory_store, executor)

    with tab_health:
        _render_health_timeline(store)

    with tab_incidents:
        _render_incidents(store, memory_store, executor)

    with tab_audit:
        _render_audit_log(store)


# ── Pipeline Flow ──────────────────────────────────────────────────────────

# Fault presets keyed by the selectbox label.
_FAULT_PRESETS = {
    "None (clean run)": None,
    "row_drop (data: volume)": ("row_drop", "", {"drop_pct": 0.6}),
    "column_null (data: nulls)": ("column_null", "amount", {"null_pct": 0.3}),
    "schema_change (data: schema)": ("schema_change", "oldbalanceOrg", {"action": "drop"}),
    "distribution_shift (data: drift)": ("distribution_shift", "amount", {"factor": 10.0}),
    "stale_data (data: freshness)": ("stale_data", "", {"stale_days": 7}),
    "operational_cause (pipeline → data)": (
        "operational_cause", "enriched",
        {"job_status": "failed", "downstream_fault_type": "row_drop",
         "downstream_target": "rows", "downstream_params": {"drop_pct": 0.85}},
    ),
    "duplicate_rows (data: uniqueness)": ("duplicate_rows", "", {"dup_pct": 0.3}),
    "out_of_range (data: validity)": ("out_of_range", "amount", {"pct": 0.1, "value": -1.0}),
    "oom (pipeline: out-of-memory)": ("oom", "enriched", {}),
    "timeout (pipeline: timeout)": ("timeout", "enriched", {}),
    "slow_job (pipeline: compute)": ("slow_job", "enriched", {}),
    "retry_storm (pipeline: retries)": ("retry_storm", "cleaned_typed", {"retries": 5}),
}


def _render_pipeline_flow(store: SentinelStore, memory_store: MemoryStore, executor: ActionExecutor):
    st.subheader("Live Pipeline Flow")
    st.caption("Watch a batch flow through the stages; faults light up where they break — "
               "🟠 data error · 🔴 pipeline error · ╌▶ correlation.")

    intent = load_intent("transactions")

    # ── Controls: run a live batch ─────────────────────────────────────
    c1, c2, c3 = st.columns([3, 1, 1])
    fault_label = c1.selectbox("Inject fault", list(_FAULT_PRESETS.keys()), key="flow_fault")
    use_llm = c2.checkbox("LLM report", value=True, key="flow_use_llm",
                          help="Generate an LLM root-cause report (needs ANTHROPIC_API_KEY); untick for faster rules-only runs.")
    run_clicked = c3.button("▶ Run a batch", key="flow_run", use_container_width=True)

    if run_clicked:
        day = st.session_state.get("flow_live_day", 600)
        st.session_state["flow_live_day"] = day + 1
        preset = _FAULT_PRESETS[fault_label]
        fault_spec = None
        if preset is not None:
            ftype, target, params = preset
            fault_spec = FaultSpec(fault_type=ftype, target=target, params=params, seed=day)

        reporter = None
        if use_llm:
            try:
                from reasoning.reporter import Reporter
                reporter = Reporter()
            except Exception as exc:
                st.warning(f"LLM disabled: {exc}")

        with st.spinner(f"Running batch ({fault_label})…"):
            manifest = run_pipeline(day=day, fault_spec=fault_spec)
            process_run(manifest, store, intent, memory_store=memory_store,
                        reporter=reporter, auto_resolve=False)
        st.session_state["flow_run_id"] = manifest["run_id"]
        # Point the (keyed) run picker at the new run so the view follows it.
        st.session_state["flow_pick_run"] = manifest["run_id"]
        st.success(f"Ran batch → run_id `{manifest['run_id']}`")

    # ── Run picker (revisit any run) ───────────────────────────────────
    recent = store.get_recent_run_ids("transactions", 25)
    if not recent:
        st.info("No runs yet — click **▶ Run a batch** to begin.")
        return
    default_run = st.session_state.get("flow_run_id")
    idx = recent.index(default_run) if default_run in recent else 0
    active_run = st.selectbox("Run", recent, index=idx, key="flow_pick_run")

    # ── Build per-stage state from ops signals + incidents ─────────────
    ops = {s.job_name: s for s in store.get_ops_signals(active_run)}
    incidents = store.get_incidents_for_run(active_run)

    # Incidents that are handled no longer represent a live error on the graph.
    _CLEARED = {
        IncidentStatus.resolved,
        IncidentStatus.suppressed,
        IncidentStatus.acknowledged_manual,
    }

    stage_states = []
    for stage in _STAGE_ORDER:
        sig = ops.get(stage)
        ops_failed = sig is not None and sig.status.value in {"failed", "skipped"}

        stage_incs = [inc for inc in incidents if inc.stage == stage]
        active_incs = [inc for inc in stage_incs if inc.status not in _CLEARED]
        active_anoms = [a for inc in active_incs for a in inc.anomalies]

        data_badges = [
            f"{a.check_type.value}: {a.metric.split('.')[-1]}"
            for a in active_anoms if a.check_type.value in _DATA_CHECKS
        ]
        op_anoms = [a for a in active_anoms if a.check_type.value == "operational"]
        op_badges = [_op_badge(a) for a in op_anoms]

        if (ops_failed or op_anoms) and active_incs:
            status = "pipeline_error"
            badges = op_badges or ["job failed"]
        elif data_badges:
            status = "data_error"
            badges = data_badges
        elif stage_incs:           # had issues, but all resolved/suppressed/acknowledged
            status = "healthy"
            badges = ["✓ resolved"]
        else:
            status = "healthy"
            badges = []

        stage_states.append({
            "id": stage, "label": _STAGE_LABELS[stage], "status": status,
            "badges": sorted(set(badges))[:3], "correlated_from": None,
        })

    # Correlation: link the operational-error stage to downstream data errors.
    op_idx = next((i for i, s in enumerate(stage_states) if s["status"] == "pipeline_error"), None)
    if op_idx is not None:
        for s in stage_states[op_idx + 1:]:
            if s["status"] == "data_error":
                s["correlated_from"] = _STAGE_ORDER[op_idx]

    components.html(render_flow(stage_states, height=460), height=480, scrolling=False)

    # ── Incidents for this run (reuse the incident card) ───────────────
    st.markdown(f"#### Incidents for run `{active_run}`")
    if not incidents:
        st.success("No incidents — clean run ✅")
    else:
        for inc in incidents:
            _render_incident_card(inc, store, memory_store, executor, ns="flow")


# ── Health Timeline ──────────────────────────────────────────────────────

def _render_health_timeline(store: SentinelStore):
    st.subheader("Pipeline Health")

    datasets = ["transactions"]
    stages = ["raw_transactions", "cleaned_typed", "enriched", "fraud_scoring_features"]

    col1, col2 = st.columns(2)
    dataset = col1.selectbox("Dataset", datasets, key="health_dataset")
    stage = col2.selectbox("Stage", stages, key="health_stage")

    metrics_list = store.get_recent_metrics(dataset, stage, n=50)

    if not metrics_list:
        st.info("No metrics data yet. Run the pipeline first.")
        return

    metrics_list.sort(key=lambda m: m.ts_run)
    timestamps = [m.ts_run for m in metrics_list]
    row_counts = [m.row_count for m in metrics_list]
    freshness = [m.freshness_minutes for m in metrics_list]

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        subplot_titles=("Row Count", "Freshness (minutes)"),
        vertical_spacing=0.12,
    )

    fig.add_trace(
        go.Scatter(
            x=timestamps, y=row_counts,
            mode="lines+markers",
            name="Row Count",
            line=dict(color="#3b82f6"),
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=timestamps, y=freshness,
            mode="lines+markers",
            name="Freshness",
            line=dict(color="#f59e0b"),
        ),
        row=2, col=1,
    )

    fig.update_layout(height=500, showlegend=False, template="plotly_dark")
    st.plotly_chart(fig, use_container_width=True)

    # Null rates
    if metrics_list:
        latest = metrics_list[-1]
        if latest.null_rate:
            st.subheader("Null Rates (Latest Run)")
            cols = st.columns(min(len(latest.null_rate), 4))
            for i, (col_name, rate) in enumerate(latest.null_rate.items()):
                with cols[i % len(cols)]:
                    color = "normal" if rate < 0.05 else "inverse"
                    st.metric(col_name, f"{rate:.2%}", delta=None)


# ── Incidents ────────────────────────────────────────────────────────────

def _render_incidents(store: SentinelStore, memory_store: MemoryStore, executor: ActionExecutor):
    st.subheader("Incident Feed")

    # Fetch every incident (all statuses), newest first by created_at.
    all_incidents: list[Incident] = []
    try:
        rows = store.conn.execute("SELECT data FROM incidents LIMIT 500").fetchall()
        for row in rows:
            try:
                all_incidents.append(Incident.model_validate_json(row[0]))
            except Exception:
                continue
        all_incidents.sort(key=lambda i: i.created_at, reverse=True)
    except Exception:
        all_incidents = []

    if not all_incidents:
        st.info("No incidents recorded yet.")
        return

    # Summary metrics — one bucket per status so every resolution gives feedback.
    def _count(*statuses) -> int:
        s = set(statuses)
        return sum(1 for i in all_incidents if i.status in s)

    cols = st.columns(7)
    cols[0].metric("🔴 Open", _count(IncidentStatus.open, IncidentStatus.awaiting_approval))
    cols[1].metric("✅ Resolved", _count(IncidentStatus.resolved))
    cols[2].metric("🔇 Suppressed", _count(IncidentStatus.suppressed))
    cols[3].metric("💤 Snoozed", _count(IncidentStatus.snoozed))
    cols[4].metric("🔧 Manual", _count(IncidentStatus.acknowledged_manual))
    cols[5].metric("❌ Rejected", _count(IncidentStatus.rejected, IncidentStatus.report_invalid))
    cols[6].metric("📊 Total", len(all_incidents))

    st.divider()

    # Render each incident
    for incident in all_incidents:
        _render_incident_card(incident, store, memory_store, executor, ns="inc")


def _render_incident_card(
    incident: Incident,
    store: SentinelStore,
    memory_store: MemoryStore,
    executor: ActionExecutor,
    ns: str = "inc",
):
    severity = incident.report.severity.value if incident.report else "unknown"
    sev_emoji = {"low": "🟢", "medium": "🟡", "high": "🟠", "critical": "🔴"}.get(severity, "⚪")

    with st.expander(
        f"{sev_emoji} **{incident.dataset}/{incident.stage}** — "
        f"`{incident.status.value}` | {incident.incident_id[:8]}",
        expanded=(incident.status in {IncidentStatus.open, IncidentStatus.awaiting_approval}),
    ):
        # Header
        c1, c2, c3 = st.columns([2, 1, 1])
        c1.markdown(f"**Run:** `{incident.run_id}`")
        c2.markdown(f"**Created:** {incident.created_at.strftime('%Y-%m-%d %H:%M')}")
        c3.markdown(f"**Status:** `{incident.status.value}`")

        # Anomalies
        if incident.anomalies:
            st.markdown("**Anomalies:**")
            for a in incident.anomalies:
                st.markdown(
                    f"- `{a.metric}` ({a.check_type.value}): "
                    f"observed=`{a.observed}` expected=`{a.expected}` "
                    f"deviation=`{a.deviation:.2f}` severity=`{a.severity_hint.value}`"
                )

        # Report
        if incident.report:
            report = incident.report
            st.markdown("---")
            st.markdown(f"**Root Cause:** {report.likely_root_cause}")
            st.markdown(f"**Caused By:** `{report.caused_by.value}`")
            st.markdown(f"**Confidence:** {report.confidence:.0%}")
            if report.evidence:
                st.markdown("**Evidence:**")
                for e in report.evidence:
                    st.markdown(f"  - {e}")

            # Suggested action
            action = report.suggested_action
            st.markdown(f"**Suggested Action:** `{action.type.value}` → `{action.target}`")
            st.markdown(f"**Rationale:** {action.rationale}")

        # Resolution controls for actionable incidents. Snoozed incidents are
        # re-surfaced (spec §7.9: defer → re-surface later) so they can be acted on.
        if incident.status in {
            IncidentStatus.open,
            IncidentStatus.awaiting_approval,
            IncidentStatus.snoozed,
        }:
            st.markdown("---")
            st.markdown("### Resolution")
            _render_resolution_controls(incident, store, memory_store, executor, ns=ns)

        # Outcome
        if incident.outcome:
            st.markdown("---")
            o = incident.outcome
            st.markdown(
                f"**Outcome:** {'✅ Resolved' if o.resolved else '❌ Unresolved'} | "
                f"Method: `{o.resolution_method.value}` | "
                f"Fix worked: `{o.fix_worked}` | "
                f"TTR: {o.time_to_resolution_minutes:.0f} min"
            )


def _resolve_via_action(incident: Incident, store: SentinelStore, memory_store: MemoryStore) -> None:
    """Mark an incident resolved after an approved action executed.

    Records an Outcome (resolution_method=action), flips status to resolved,
    writes a MemoryRecord (closing the loop into Memory per spec §10), and
    audits the outcome.  The action types offered (rerun_job / quarantine_batch)
    are reversible and directly address the anomaly, so fix_worked=True.
    """
    now = datetime.now(timezone.utc)
    created = incident.created_at
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    ttr = round((now - created).total_seconds() / 60.0, 2)

    outcome = Outcome(
        incident_id=incident.incident_id,
        resolved=True,
        resolved_at=now,
        time_to_resolution_minutes=ttr,
        resolution_method=ResolutionMethod.action,
        fix_worked=True,
    )
    incident.status = IncidentStatus.resolved
    incident.outcome = outcome
    store.save_outcome(outcome)
    store.update_incident(incident)

    try:
        check_type = incident.anomalies[0].check_type.value if incident.anomalies else "unknown"
        memory_store.add_record(MemoryRecord(
            incident_id=incident.incident_id,
            dataset=incident.dataset,
            check_type=check_type,
            summary_text=build_summary_text(incident),
            report=incident.report,
            outcome=outcome,
        ))
    except Exception:
        pass  # memory write is best-effort

    log_event(
        store,
        event=AuditEvent.outcome_recorded,
        incident_id=incident.incident_id,
        detail={"resolved": True, "resolution_method": "action", "fix_worked": True,
                "time_to_resolution_minutes": ttr},
        actor=ActorType.human,
    )


def _render_resolution_controls(
    incident: Incident,
    store: SentinelStore,
    memory_store: MemoryStore,
    executor: ActionExecutor,
    ns: str = "inc",
):
    """Render approve/reject/modify/snooze buttons."""

    # Namespace widget keys by render context (tab); the same incident can be
    # shown in multiple tabs, and Streamlit executes every tab each run.
    key_prefix = f"{ns}_{incident.incident_id[:8]}"

    # Action preview
    if incident.report and incident.report.suggested_action.type not in {ActionType.none, ActionType.manual}:
        action = incident.report.suggested_action
        action_def = get_action(action.type)

        if action_def and not is_blocked(action.type):
            preview = preview_action(action.type, action.target, incident.dataset)
            st.info(
                f"**Preview:** {preview.description}\n\n"
                f"Risk: `{preview.risk_tier}` | Reversible: `{preview.reversible}` | "
                f"Effect: {preview.estimated_effect}"
            )

            # Determine gate type
            from intent.parser import load_intent
            try:
                intent = load_intent(incident.dataset)
                gate = evaluate_gate(action_def, intent.criticality)
            except Exception:
                gate = action_def.gate

            col1, col2, col3, col4 = st.columns(4)

            # Approve button
            with col1:
                if gate == GateType.one_click:
                    approve_label = "✅ Approve (1-click)"
                else:
                    approve_label = "✅ Approve (confirm)"

                if st.button(approve_label, key=f"{key_prefix}_approve"):
                    resolution = Resolution(
                        incident_id=incident.incident_id,
                        decision=DecisionType.approved,
                        reason=ReasonCode.none,
                    )
                    updated = process_resolution(resolution, incident, store, memory_store)
                    # Execute the action
                    result = executor.execute(action.type, action.target, incident.run_id)
                    log_event(store, AuditEvent.action_executed, incident.incident_id, result)
                    # Record the outcome and resolve (closes the loop into Memory).
                    _resolve_via_action(updated, store, memory_store)
                    st.success(f"Action executed & incident resolved: {result.get('description', 'Done')}")
                    st.rerun()

            # Reject: Not a Problem
            with col2:
                if st.button("🔇 Not a Problem", key=f"{key_prefix}_not_problem"):
                    resolution = Resolution(
                        incident_id=incident.incident_id,
                        decision=DecisionType.rejected,
                        reason=ReasonCode.not_a_problem,
                    )
                    process_resolution(resolution, incident, store, memory_store)
                    st.success("Marked as not a problem. Suppression rule created.")
                    st.rerun()

            # Reject: Will Fix Manually
            with col3:
                if st.button("🔧 Fix Manually", key=f"{key_prefix}_fix_manual"):
                    st.session_state[f"{key_prefix}_show_manual"] = True

            # Snooze
            with col4:
                if st.button("💤 Snooze", key=f"{key_prefix}_snooze"):
                    resolution = Resolution(
                        incident_id=incident.incident_id,
                        decision=DecisionType.snoozed,
                        reason=ReasonCode.defer,
                    )
                    process_resolution(resolution, incident, store, memory_store)
                    st.success("Incident snoozed.")
                    st.rerun()

            # Manual fix note input
            if st.session_state.get(f"{key_prefix}_show_manual"):
                note = st.text_area("Describe how you'll fix this:", key=f"{key_prefix}_note")
                if st.button("Submit Fix Note", key=f"{key_prefix}_submit_note"):
                    resolution = Resolution(
                        incident_id=incident.incident_id,
                        decision=DecisionType.rejected,
                        reason=ReasonCode.will_fix_manually,
                        manual_fix_note=note,
                    )
                    process_resolution(resolution, incident, store, memory_store)
                    st.success("Manual fix acknowledged.")
                    st.session_state.pop(f"{key_prefix}_show_manual", None)
                    st.rerun()

            # Wrong diagnosis button
            if st.button("❌ Wrong Diagnosis", key=f"{key_prefix}_wrong"):
                resolution = Resolution(
                    incident_id=incident.incident_id,
                    decision=DecisionType.rejected,
                    reason=ReasonCode.wrong_diagnosis,
                )
                process_resolution(resolution, incident, store, memory_store)
                st.warning("Marked as wrong diagnosis. Negative signal recorded.")
                st.rerun()
        else:
            st.warning("Suggested action is blocked by policy. Manual intervention required.")
    else:
        st.info("No automated action suggested. Manual resolution options below.")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔇 Not a Problem", key=f"{key_prefix}_np2"):
                resolution = Resolution(
                    incident_id=incident.incident_id,
                    decision=DecisionType.rejected,
                    reason=ReasonCode.not_a_problem,
                )
                process_resolution(resolution, incident, store, memory_store)
                st.rerun()
        with col2:
            if st.button("💤 Snooze", key=f"{key_prefix}_snooze2"):
                resolution = Resolution(
                    incident_id=incident.incident_id,
                    decision=DecisionType.snoozed,
                    reason=ReasonCode.defer,
                )
                process_resolution(resolution, incident, store, memory_store)
                st.rerun()
        with col3:
            if st.button("❌ Wrong Diagnosis", key=f"{key_prefix}_wrong2"):
                resolution = Resolution(
                    incident_id=incident.incident_id,
                    decision=DecisionType.rejected,
                    reason=ReasonCode.wrong_diagnosis,
                )
                process_resolution(resolution, incident, store, memory_store)
                st.rerun()


# ── Audit Log ────────────────────────────────────────────────────────────

def _render_audit_log(store: SentinelStore):
    st.subheader("Audit Log")

    try:
        rows = store.conn.execute(
            "SELECT entry_id, ts, incident_id, event, actor, detail "
            "FROM audit_log ORDER BY ts DESC LIMIT 100"
        ).fetchall()
    except Exception:
        rows = []

    if not rows:
        st.info("No audit entries yet.")
        return

    import pandas as pd
    df = pd.DataFrame(rows, columns=["Entry ID", "Timestamp", "Incident ID", "Event", "Actor", "Detail"])
    st.dataframe(df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
