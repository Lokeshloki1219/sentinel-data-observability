"""
Sentinel — Action Executor (§12).

Executes approved actions and provides undo capabilities.
Sandboxed to orchestrator + warehouse operations only.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

import duckdb

from schemas import ActionType

logger = logging.getLogger(__name__)


class ActionExecutor:
    """Executes approved actions against the warehouse / orchestrator."""

    def __init__(self, store) -> None:
        """
        Args:
            store: SentinelStore instance for warehouse operations.
        """
        self._store = store

    # ── public API ────────────────────────────────────────────────────

    def execute(self, action_type: ActionType, target: str, run_id: str) -> Dict[str, Any]:
        """Execute an approved action.

        Args:
            action_type: The type of action to execute.
            target: Action target (job name, batch id, etc.).
            run_id: The run that triggered the incident.

        Returns:
            Dict with execution result details.
        """
        handler = {
            ActionType.rerun_job: self._execute_rerun_job,
            ActionType.quarantine_batch: self._execute_quarantine_batch,
        }.get(action_type)

        if handler is None:
            return {
                "success": False,
                "action_type": action_type.value,
                "error": f"No executor implemented for action type '{action_type.value}'",
            }

        try:
            result = handler(target, run_id)
            result["success"] = True
            result["action_type"] = action_type.value
            result["executed_at"] = datetime.now(timezone.utc).isoformat()
            logger.info("Action executed: %s target=%s run=%s", action_type.value, target, run_id)
            return result
        except Exception as exc:
            logger.exception("Action execution failed: %s", action_type.value)
            return {
                "success": False,
                "action_type": action_type.value,
                "error": str(exc),
            }

    def undo(self, action_type: ActionType, target: str, run_id: str) -> Dict[str, Any]:
        """Reverse a previously executed action.

        Args:
            action_type: The type of action to undo.
            target: Action target (job name, batch id, etc.).
            run_id: The original run id.

        Returns:
            Dict with undo result details.
        """
        handler = {
            ActionType.rerun_job: self._undo_rerun_job,
            ActionType.quarantine_batch: self._undo_quarantine_batch,
        }.get(action_type)

        if handler is None:
            return {
                "success": False,
                "action_type": action_type.value,
                "error": f"No undo implemented for action type '{action_type.value}'",
            }

        try:
            result = handler(target, run_id)
            result["success"] = True
            result["action_type"] = action_type.value
            result["undone_at"] = datetime.now(timezone.utc).isoformat()
            logger.info("Action undone: %s target=%s run=%s", action_type.value, target, run_id)
            return result
        except Exception as exc:
            logger.exception("Action undo failed: %s", action_type.value)
            return {
                "success": False,
                "action_type": action_type.value,
                "error": str(exc),
            }

    # ── rerun_job ─────────────────────────────────────────────────────

    def _execute_rerun_job(self, target: str, run_id: str) -> Dict[str, Any]:
        """Re-trigger a pipeline job.

        In a production system this would call the orchestrator API
        (Prefect / Airflow). Here we log the intent and record it.
        """
        logger.info("RERUN_JOB: Re-triggering job '%s' for run '%s'", target, run_id)
        return {
            "description": f"Re-triggered job '{target}'",
            "target": target,
            "run_id": run_id,
            "note": "In production, this calls the orchestrator API to re-trigger the job.",
        }

    def _undo_rerun_job(self, target: str, run_id: str) -> Dict[str, Any]:
        """Undo a rerun — effectively a no-op since the job already ran."""
        logger.info("UNDO RERUN_JOB: No-op — job '%s' already executed.", target)
        return {
            "description": f"No-op: job '{target}' already executed and cannot be un-run.",
            "target": target,
            "run_id": run_id,
        }

    # ── quarantine_batch ──────────────────────────────────────────────

    def _execute_quarantine_batch(self, target: str, run_id: str) -> Dict[str, Any]:
        """Move flagged batch rows to a quarantine table in DuckDB.

        'target' is the source table name (e.g. 'raw_transactions').
        Rows matching the run_id are moved to '<table>_quarantine'.
        """
        conn: duckdb.DuckDBPyConnection = self._store.conn
        source_table = target
        quarantine_table = f"{source_table}_quarantine"

        # Ensure the quarantine table exists with the same schema
        conn.execute(
            f"CREATE TABLE IF NOT EXISTS {quarantine_table} AS "
            f"SELECT * FROM {source_table} WHERE 1=0"
        )

        # Move rows: insert into quarantine, then delete from source
        moved = conn.execute(
            f"INSERT INTO {quarantine_table} SELECT * FROM {source_table} "
            f"WHERE run_id = ?",
            [run_id],
        ).fetchone()

        conn.execute(
            f"DELETE FROM {source_table} WHERE run_id = ?",
            [run_id],
        )

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {quarantine_table} WHERE run_id = ?",
            [run_id],
        ).fetchone()[0]

        logger.info(
            "QUARANTINE_BATCH: Moved %d rows from '%s' to '%s' for run '%s'",
            row_count, source_table, quarantine_table, run_id,
        )
        return {
            "description": f"Quarantined {row_count} rows from '{source_table}'",
            "target": source_table,
            "quarantine_table": quarantine_table,
            "run_id": run_id,
            "rows_moved": row_count,
        }

    def _undo_quarantine_batch(self, target: str, run_id: str) -> Dict[str, Any]:
        """Un-quarantine: move rows back from quarantine table to source.

        This is the explicit undo for quarantine_batch per spec §12.
        """
        conn: duckdb.DuckDBPyConnection = self._store.conn
        source_table = target
        quarantine_table = f"{source_table}_quarantine"

        # Move rows back: insert into source, delete from quarantine
        conn.execute(
            f"INSERT INTO {source_table} SELECT * FROM {quarantine_table} "
            f"WHERE run_id = ?",
            [run_id],
        )

        row_count = conn.execute(
            f"SELECT COUNT(*) FROM {quarantine_table} WHERE run_id = ?",
            [run_id],
        ).fetchone()[0]

        conn.execute(
            f"DELETE FROM {quarantine_table} WHERE run_id = ?",
            [run_id],
        )

        logger.info(
            "UN-QUARANTINE: Moved %d rows back from '%s' to '%s' for run '%s'",
            row_count, quarantine_table, source_table, run_id,
        )
        return {
            "description": f"Un-quarantined {row_count} rows back to '{source_table}'",
            "target": source_table,
            "quarantine_table": quarantine_table,
            "run_id": run_id,
            "rows_restored": row_count,
        }
