"""
Sentinel — DuckDB Persistence Layer.

Provides :class:`SentinelStore`, the single read/write gateway for every
persistent artefact in the Sentinel observability system.  All tables use a
``data`` JSON column that holds the full Pydantic-serialised model so that
the schema can evolve without DDL migrations.
"""

from __future__ import annotations

import logging
import re
from typing import List, Optional

import duckdb
import pandas as pd

from schemas import (
    AuditEntry,
    Incident,
    OperationalSignals,
    Outcome,
    Resolution,
    RunMetrics,
    SuppressionRule,
)

logger = logging.getLogger(__name__)


class SentinelStore:
    """DuckDB-backed store for all Sentinel observability data.

    Parameters
    ----------
    db_path : str
        Filesystem path to the DuckDB database file.  Use ``":memory:"``
        for ephemeral / test databases.
    """

    # ── lifecycle ──────────────────────────────────────────────────────────

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.con = duckdb.connect(db_path)
        self._init_tables()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Alias for ``self.con`` used by external modules."""
        return self.con

    def _init_tables(self) -> None:
        """Create all required tables if they do not already exist."""
        ddl_statements = [
            # 1. run_metrics
            """
            CREATE TABLE IF NOT EXISTS run_metrics (
                run_id    VARCHAR NOT NULL,
                dataset   VARCHAR NOT NULL,
                stage     VARCHAR NOT NULL,
                ts_run    TIMESTAMP NOT NULL,
                data      JSON NOT NULL,
                PRIMARY KEY (run_id, stage)
            )
            """,
            # 2. ops_signals
            """
            CREATE TABLE IF NOT EXISTS ops_signals (
                run_id    VARCHAR NOT NULL,
                job_name  VARCHAR NOT NULL,
                status    VARCHAR NOT NULL,
                data      JSON NOT NULL,
                PRIMARY KEY (run_id, job_name)
            )
            """,
            # 3. incidents
            """
            CREATE TABLE IF NOT EXISTS incidents (
                incident_id VARCHAR NOT NULL PRIMARY KEY,
                dataset     VARCHAR NOT NULL,
                stage       VARCHAR NOT NULL,
                run_id      VARCHAR NOT NULL,
                status      VARCHAR NOT NULL,
                data        JSON NOT NULL
            )
            """,
            # 4. audit_log
            """
            CREATE TABLE IF NOT EXISTS audit_log (
                entry_id    VARCHAR NOT NULL PRIMARY KEY,
                ts          TIMESTAMP NOT NULL,
                incident_id VARCHAR,
                event       VARCHAR NOT NULL,
                actor       VARCHAR NOT NULL,
                detail      JSON NOT NULL
            )
            """,
            # 5. suppression_rules
            """
            CREATE TABLE IF NOT EXISTS suppression_rules (
                rule_id    VARCHAR NOT NULL PRIMARY KEY,
                dataset    VARCHAR NOT NULL,
                metric     VARCHAR NOT NULL,
                check_type VARCHAR NOT NULL,
                effect     VARCHAR NOT NULL,
                data       JSON NOT NULL
            )
            """,
            # 6. resolutions
            """
            CREATE TABLE IF NOT EXISTS resolutions (
                incident_id VARCHAR NOT NULL PRIMARY KEY,
                decision    VARCHAR NOT NULL,
                reason      VARCHAR NOT NULL,
                data        JSON NOT NULL
            )
            """,
            # 7. outcomes
            """
            CREATE TABLE IF NOT EXISTS outcomes (
                incident_id VARCHAR NOT NULL PRIMARY KEY,
                resolved    BOOLEAN NOT NULL,
                data        JSON NOT NULL
            )
            """,
            # 8. anomaly_streaks — debounce bookkeeping (Section 11)
            """
            CREATE TABLE IF NOT EXISTS anomaly_streaks (
                dataset      VARCHAR NOT NULL,
                stage        VARCHAR NOT NULL,
                metric       VARCHAR NOT NULL,
                streak       INTEGER NOT NULL,
                last_run_id  VARCHAR NOT NULL,
                PRIMARY KEY (dataset, stage, metric)
            )
            """,
        ]
        for ddl in ddl_statements:
            self.con.execute(ddl)
        logger.info("SentinelStore tables initialised at %s", self.db_path)

    # ── Batch data (warehouse tables) ──────────────────────────────────────

    @staticmethod
    def _safe_table(name: str) -> str:
        """Whitelist a stage name to a safe DuckDB identifier.

        Stage names come from our own pipeline config, but we sanitise anyway
        so the dynamic ``CREATE TABLE`` can never be an injection vector.
        """
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Unsafe table name: {name!r}")
        return name

    def save_batch(self, stage: str, run_id: str, batch: "pd.DataFrame") -> None:
        """Persist a stage's batch rows to a warehouse table tagged with run_id.

        Each stage gets its own table (named after the stage).  A ``run_id``
        column is added so a later :class:`~action.executor.ActionExecutor`
        can quarantine exactly the rows produced by one run.
        """
        table = self._safe_table(stage)
        df = batch.copy()
        df["run_id"] = run_id

        # Register the DataFrame and create/append using DuckDB's pandas bridge.
        self.con.register("_incoming_batch", df)
        self.con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM _incoming_batch WHERE 1=0"
        )
        self.con.execute(f"INSERT INTO {table} SELECT * FROM _incoming_batch")
        self.con.unregister("_incoming_batch")
        logger.debug("Saved %d rows to warehouse table '%s' (run=%s)", len(df), table, run_id)

    def get_batch(self, stage: str, run_id: str) -> "pd.DataFrame":
        """Return the rows of *stage* produced by *run_id* (empty if none)."""
        table = self._safe_table(stage)
        try:
            return self.con.execute(
                f"SELECT * FROM {table} WHERE run_id = ?", [run_id]
            ).fetchdf()
        except duckdb.CatalogException:
            return pd.DataFrame()

    # ── RunMetrics ─────────────────────────────────────────────────────────

    def save_metrics(self, m: RunMetrics) -> None:
        """Insert a new RunMetrics snapshot."""
        self.con.execute(
            """
            INSERT INTO run_metrics (run_id, dataset, stage, ts_run, data)
            VALUES (?, ?, ?, ?, ?)
            """,
            [m.run_id, m.dataset, m.stage, m.ts_run, m.model_dump_json(by_alias=True)],
        )

    def get_recent_metrics(
        self, dataset: str, stage: str, n: int = 30
    ) -> List[RunMetrics]:
        """Return the *n* most recent RunMetrics for a (dataset, stage) pair.

        Results are ordered newest-first so ``history[0]`` is the latest
        previous run.
        """
        rows = self.con.execute(
            """
            SELECT data FROM run_metrics
            WHERE dataset = ? AND stage = ?
            ORDER BY ts_run DESC
            LIMIT ?
            """,
            [dataset, stage, n],
        ).fetchall()
        return [RunMetrics.model_validate_json(row[0]) for row in rows]

    # ── OperationalSignals ─────────────────────────────────────────────────

    def save_ops_signals(self, s: OperationalSignals) -> None:
        """Persist an OperationalSignals record."""
        self.con.execute(
            """
            INSERT INTO ops_signals (run_id, job_name, status, data)
            VALUES (?, ?, ?, ?)
            """,
            [s.run_id, s.job_name, s.status.value, s.model_dump_json(by_alias=True)],
        )

    def get_ops_signals(self, run_id: str) -> List[OperationalSignals]:
        """Return all operational signals for a given run."""
        rows = self.con.execute(
            "SELECT data FROM ops_signals WHERE run_id = ?", [run_id]
        ).fetchall()
        return [OperationalSignals.model_validate_json(row[0]) for row in rows]

    def get_recent_ops(self, job_name: str, n: int = 30) -> List[OperationalSignals]:
        """Return recent OperationalSignals for a job, newest-first.

        Used as the rolling baseline for operational anomaly detection
        (duration spikes, etc.).  Ordered by ``started_at`` descending.
        """
        rows = self.con.execute(
            "SELECT data FROM ops_signals WHERE job_name = ?", [job_name]
        ).fetchall()
        sigs = [OperationalSignals.model_validate_json(r[0]) for r in rows]
        sigs.sort(key=lambda s: s.started_at, reverse=True)
        return sigs[:n]

    # ── Incidents ──────────────────────────────────────────────────────────

    def save_incident(self, i: Incident) -> None:
        """Insert a new Incident."""
        self.con.execute(
            """
            INSERT INTO incidents (incident_id, dataset, stage, run_id, status, data)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                i.incident_id,
                i.dataset,
                i.stage,
                i.run_id,
                i.status.value,
                i.model_dump_json(by_alias=True),
            ],
        )

    def update_incident(self, i: Incident) -> None:
        """Update an existing Incident (status changes, added report, etc.)."""
        self.con.execute(
            """
            UPDATE incidents
            SET status = ?, data = ?
            WHERE incident_id = ?
            """,
            [i.status.value, i.model_dump_json(by_alias=True), i.incident_id],
        )

    def get_open_incidents(self) -> List[Incident]:
        """Return all incidents that are not yet resolved or suppressed."""
        rows = self.con.execute(
            """
            SELECT data FROM incidents
            WHERE status NOT IN ('resolved', 'suppressed', 'report_invalid')
            ORDER BY incident_id
            """
        ).fetchall()
        return [Incident.model_validate_json(row[0]) for row in rows]

    def get_incident(self, incident_id: str) -> Optional[Incident]:
        """Fetch a single incident by ID, or ``None`` if not found."""
        rows = self.con.execute(
            "SELECT data FROM incidents WHERE incident_id = ?", [incident_id]
        ).fetchall()
        if not rows:
            return None
        return Incident.model_validate_json(rows[0][0])

    def get_incidents_for_run(self, run_id: str) -> List[Incident]:
        """Return all incidents created from a given pipeline run."""
        rows = self.con.execute(
            "SELECT data FROM incidents WHERE run_id = ?", [run_id]
        ).fetchall()
        return [Incident.model_validate_json(r[0]) for r in rows]

    def get_recent_run_ids(self, dataset: str, n: int = 25) -> List[str]:
        """Return recent distinct run_ids for a dataset, newest-first.

        Ordered by the latest ``ts_run`` seen for each run so the dashboard's
        run picker lists the most recent pipeline executions first.
        """
        rows = self.con.execute(
            """
            SELECT run_id, MAX(ts_run) AS last_ts
            FROM run_metrics
            WHERE dataset = ?
            GROUP BY run_id
            ORDER BY last_ts DESC
            LIMIT ?
            """,
            [dataset, n],
        ).fetchall()
        return [r[0] for r in rows]

    # ── AuditEntry ─────────────────────────────────────────────────────────

    def save_audit(self, e: AuditEntry) -> None:
        """Append an entry to the immutable audit log."""
        self.con.execute(
            """
            INSERT INTO audit_log (entry_id, ts, incident_id, event, actor, detail)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                e.entry_id,
                e.ts,
                e.incident_id,
                e.event.value,
                e.actor.value,
                e.model_dump_json(by_alias=True),
            ],
        )

    # ── SuppressionRules ───────────────────────────────────────────────────

    def save_suppression_rule(self, r: SuppressionRule) -> None:
        """Persist a new suppression rule."""
        self.con.execute(
            """
            INSERT INTO suppression_rules (rule_id, dataset, metric, check_type, effect, data)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                r.rule_id,
                r.dataset,
                r.match.metric,
                r.match.check_type,
                r.effect.value,
                r.model_dump_json(by_alias=True),
            ],
        )

    def get_active_suppressions(self, dataset: str) -> List[SuppressionRule]:
        """Return all suppression rules that apply to *dataset*."""
        rows = self.con.execute(
            "SELECT data FROM suppression_rules WHERE dataset = ?", [dataset]
        ).fetchall()
        return [SuppressionRule.model_validate_json(row[0]) for row in rows]

    # ── Resolution / Outcome ───────────────────────────────────────────────

    def save_resolution(self, r: Resolution) -> None:
        """Persist a Resolution decision."""
        self.con.execute(
            """
            INSERT INTO resolutions (incident_id, decision, reason, data)
            VALUES (?, ?, ?, ?)
            """,
            [
                r.incident_id,
                r.decision.value,
                r.reason.value,
                r.model_dump_json(by_alias=True),
            ],
        )

    def save_outcome(self, o: Outcome) -> None:
        """Persist an Outcome record."""
        self.con.execute(
            """
            INSERT INTO outcomes (incident_id, resolved, data)
            VALUES (?, ?, ?)
            """,
            [o.incident_id, o.resolved, o.model_dump_json(by_alias=True)],
        )

    # ── Anomaly streaks (debounce, Section 11) ──────────────────────────────

    def get_anomaly_streaks(self, dataset: str, stage: str) -> dict[str, int]:
        """Return ``{metric: streak}`` for a (dataset, stage) pair."""
        rows = self.con.execute(
            "SELECT metric, streak FROM anomaly_streaks WHERE dataset = ? AND stage = ?",
            [dataset, stage],
        ).fetchall()
        return {metric: streak for metric, streak in rows}

    def set_anomaly_streak(
        self, dataset: str, stage: str, metric: str, streak: int, run_id: str
    ) -> None:
        """Upsert the consecutive-anomaly streak for a metric."""
        self.con.execute(
            """
            INSERT INTO anomaly_streaks (dataset, stage, metric, streak, last_run_id)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (dataset, stage, metric)
            DO UPDATE SET streak = EXCLUDED.streak, last_run_id = EXCLUDED.last_run_id
            """,
            [dataset, stage, metric, streak, run_id],
        )

    def clear_anomaly_streaks(
        self, dataset: str, stage: str, keep_metrics: List[str]
    ) -> None:
        """Reset streaks for metrics in (dataset, stage) that are NOT in
        *keep_metrics* — i.e. metrics that were not anomalous this run, so
        their consecutive streak is broken."""
        if keep_metrics:
            placeholders = ", ".join("?" for _ in keep_metrics)
            self.con.execute(
                f"DELETE FROM anomaly_streaks WHERE dataset = ? AND stage = ? "
                f"AND metric NOT IN ({placeholders})",
                [dataset, stage, *keep_metrics],
            )
        else:
            self.con.execute(
                "DELETE FROM anomaly_streaks WHERE dataset = ? AND stage = ?",
                [dataset, stage],
            )
