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
from datetime import datetime
from typing import List, Optional

from config import config
from schemas import (
    ActionType,
    AuditEvent,
    Criticality,
    DecisionType,
    GateType,
    Incident,
    IncidentStatus,
    ReasonCode,
    Resolution,
    RunMetrics,
    SeverityLevel,
)
from observability.store import SentinelStore
from action.registry import get_action, is_blocked, ACTION_REGISTRY
from action.preview import preview_action
from action.executor import ActionExecutor
from governance.policy import evaluate_gate
from governance.approval import process_resolution
from governance.audit import log_event
from memory.store import MemoryStore


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

    tab_health, tab_incidents, tab_audit = st.tabs(
        ["📈 Health Timeline", "🚨 Incidents", "📋 Audit Log"]
    )

    with tab_health:
        _render_health_timeline(store)

    with tab_incidents:
        _render_incidents(store, memory_store, executor)

    with tab_audit:
        _render_audit_log(store)


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

    # Fetch all incidents
    try:
        all_incidents = store.get_open_incidents()
    except Exception:
        all_incidents = []

    # Also try to get resolved / other status incidents
    try:
        conn = store.conn
        rows = conn.execute(
            "SELECT data FROM incidents ORDER BY ts_created DESC LIMIT 50"
        ).fetchall()
        from schemas import Incident as Inc
        all_incidents = []
        for row in rows:
            try:
                all_incidents.append(Inc.model_validate_json(row[0]))
            except Exception:
                continue
    except Exception:
        pass

    if not all_incidents:
        st.info("No incidents recorded yet.")
        return

    # Summary metrics
    cols = st.columns(4)
    open_count = sum(1 for i in all_incidents if i.status in {IncidentStatus.open, IncidentStatus.awaiting_approval})
    resolved_count = sum(1 for i in all_incidents if i.status == IncidentStatus.resolved)
    suppressed_count = sum(1 for i in all_incidents if i.status == IncidentStatus.suppressed)
    cols[0].metric("🔴 Open", open_count)
    cols[1].metric("✅ Resolved", resolved_count)
    cols[2].metric("🔇 Suppressed", suppressed_count)
    cols[3].metric("📊 Total", len(all_incidents))

    st.divider()

    # Render each incident
    for incident in all_incidents:
        _render_incident_card(incident, store, memory_store, executor)


def _render_incident_card(
    incident: Incident,
    store: SentinelStore,
    memory_store: MemoryStore,
    executor: ActionExecutor,
):
    severity = incident.report.severity.value if incident.report else "unknown"
    sev_color = SEVERITY_COLORS.get(severity, "#6b7280")
    status_color = STATUS_COLORS.get(incident.status.value, "#6b7280")

    with st.expander(
        f":{severity.upper()}: **{incident.dataset}/{incident.stage}** — "
        f"`{incident.status.value}` | {incident.incident_id[:8]}",
        expanded=(incident.status in {IncidentStatus.open, IncidentStatus.awaiting_approval}),
    ):
        # Header
        c1, c2, c3 = st.columns([2, 1, 1])
        c1.markdown(f"**Run:** `{incident.run_id}`")
        c2.markdown(f"**Created:** {incident.created_at.strftime('%Y-%m-%d %H:%M')}")
        c3.markdown(f"**Status:** :{status_color}[{incident.status.value}]")

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

        # Resolution controls (only for open / awaiting incidents)
        if incident.status in {IncidentStatus.open, IncidentStatus.awaiting_approval}:
            st.markdown("---")
            st.markdown("### Resolution")
            _render_resolution_controls(incident, store, memory_store, executor)

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


def _render_resolution_controls(
    incident: Incident,
    store: SentinelStore,
    memory_store: MemoryStore,
    executor: ActionExecutor,
):
    """Render approve/reject/modify/snooze buttons."""

    key_prefix = incident.incident_id[:8]

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
                    updated.status = IncidentStatus.acted
                    store.update_incident(updated)
                    st.success(f"Action executed: {result.get('description', 'Done')}")
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
