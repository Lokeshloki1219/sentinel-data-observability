"""
Sentinel — Memory Vector Store (Section 8: memory/store.py).

Wraps a ChromaDB persistent collection for embedding-based storage
and retrieval of past incident records (``MemoryRecord``).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import chromadb

from schemas import MemoryRecord

logger = logging.getLogger(__name__)


class MemoryStore:
    """ChromaDB-backed vector store for incident memory.

    Uses ChromaDB's default embedding function (backed by
    ``sentence-transformers``) to embed incident summary text
    and support similarity-based retrieval.
    """

    COLLECTION_NAME: str = "sentinel_memory"

    def __init__(self, persist_dir: str) -> None:
        """Initialize ChromaDB with a persistent storage directory.

        Parameters
        ----------
        persist_dir:
            Filesystem path where ChromaDB will persist its data.
        """
        self._client = chromadb.PersistentClient(path=persist_dir)
        self._collection = self._client.get_or_create_collection(
            name=self.COLLECTION_NAME,
        )
        logger.info(
            "MemoryStore initialised — collection=%s, persist_dir=%s",
            self.COLLECTION_NAME,
            persist_dir,
        )

    @property
    def collection(self) -> chromadb.Collection:
        """Expose the underlying ChromaDB collection for direct queries."""
        return self._collection

    def add_record(self, record: MemoryRecord, is_negative: bool = False) -> None:
        """Upsert a :class:`MemoryRecord` into the vector store.

        The ``summary_text`` is used as the document for embedding.
        Metadata includes ``incident_id``, ``dataset``, ``check_type``,
        and an ``is_negative`` flag that is ``True`` when the record
        originates from a ``wrong_diagnosis`` resolution (negative
        retrieval signal).

        Parameters
        ----------
        record:
            The memory record to store.
        is_negative:
            Explicitly mark this record as a negative retrieval signal.
            Overrides the automatic detection from ``_is_negative()``.
        """
        metadata: Dict[str, Any] = {
            "incident_id": record.incident_id,
            "dataset": record.dataset,
            "check_type": record.check_type,
            "is_negative": is_negative or self._is_negative(record),
        }

        self._collection.upsert(
            ids=[record.incident_id],
            documents=[record.summary_text],
            metadatas=[metadata],
        )
        logger.debug(
            "Upserted MemoryRecord incident_id=%s (is_negative=%s)",
            record.incident_id,
            metadata["is_negative"],
        )

    def delete_record(self, incident_id: str) -> None:
        """Remove a record from the collection by incident ID.

        Parameters
        ----------
        incident_id:
            The unique identifier of the incident to remove.
        """
        self._collection.delete(ids=[incident_id])
        logger.debug("Deleted MemoryRecord incident_id=%s", incident_id)

    # ── Internal helpers ───────────────────────────────────────────────

    @staticmethod
    def _is_negative(record: MemoryRecord) -> bool:
        """Determine if a record is a negative retrieval signal.

        A record is marked negative when it originated from an incident
        whose resolution reason was ``wrong_diagnosis``.  These records
        are kept in the store so the LLM can learn to *avoid* repeating
        the same incorrect diagnosis.
        """
        if record.report is not None and record.outcome is not None:
            # If the outcome shows the fix did NOT work, treat as negative
            if record.outcome.fix_worked is False:
                return True
        return False
