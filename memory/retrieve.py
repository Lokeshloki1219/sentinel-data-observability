"""
Sentinel — Memory Retrieval (Section 8: memory/retrieve.py).

Retrieves the top-k most similar past incidents from the vector
store for a given anomaly, including **negative retrieval signals**
(prior wrong diagnoses) so the LLM can learn what NOT to repeat.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from schemas import Anomaly, MemoryRecord
from memory.store import MemoryStore

logger = logging.getLogger(__name__)

# Prefix injected into summary_text for negative records so the LLM
# can distinguish them from positive exemplars.
_NEGATIVE_PREFIX: str = (
    "[NEGATIVE SIGNAL — this was a WRONG diagnosis; avoid repeating it] "
)


def _build_query(anomaly: Anomaly) -> str:
    """Construct a text query from anomaly fields for embedding search.

    The query mirrors the structure of :func:`memory.embed.build_summary_text`
    so the embedding similarity is meaningful.

    Parameters
    ----------
    anomaly:
        The anomaly to build a query for.

    Returns
    -------
    str
        A prose-like query string.
    """
    return (
        f"Anomaly on dataset={anomaly.dataset}, "
        f"metric={anomaly.metric}, "
        f"check_type={anomaly.check_type.value}. "
        f"Observed={anomaly.observed}, expected={anomaly.expected}."
    )


def retrieve_similar(
    store: MemoryStore,
    anomaly: Anomaly,
    top_k: int = 5,
) -> List[MemoryRecord]:
    """Retrieve the most similar past incidents for an anomaly.

    Parameters
    ----------
    store:
        The :class:`MemoryStore` instance to query.
    anomaly:
        The current anomaly to find similar past incidents for.
    top_k:
        Maximum number of results to return (default 5).

    Returns
    -------
    list[MemoryRecord]
        Up to ``top_k`` memory records, ordered by similarity.
        Records with ``is_negative=True`` metadata have their
        ``summary_text`` prefixed with a warning so the LLM
        recognises them as bad diagnoses to avoid.
    """
    query_text = _build_query(anomaly)

    try:
        results = store.collection.query(
            query_texts=[query_text],
            n_results=top_k,
            include=["documents", "metadatas", "distances"],
        )
    except Exception:
        logger.exception("ChromaDB query failed for anomaly=%s", anomaly.anomaly_id)
        return []

    if not results or not results["ids"] or not results["ids"][0]:
        logger.debug("No similar incidents found for anomaly=%s", anomaly.anomaly_id)
        return []

    records: List[MemoryRecord] = []
    ids: List[str] = results["ids"][0]
    documents: List[Optional[str]] = results["documents"][0]  # type: ignore[index]
    metadatas: List[Dict[str, Any]] = results["metadatas"][0]  # type: ignore[index]

    for i, incident_id in enumerate(ids):
        meta: Dict[str, Any] = metadatas[i] if metadatas else {}
        doc: str = documents[i] if documents and documents[i] else ""

        is_negative: bool = bool(meta.get("is_negative", False))

        # Mark negative records so the LLM can distinguish them
        summary_text = (
            _NEGATIVE_PREFIX + doc if is_negative else doc
        )

        record = MemoryRecord(
            incident_id=incident_id,
            dataset=meta.get("dataset", ""),
            check_type=meta.get("check_type", ""),
            summary_text=summary_text,
        )
        records.append(record)

    logger.debug(
        "Retrieved %d similar incidents for anomaly=%s (negatives=%d)",
        len(records),
        anomaly.anomaly_id,
        sum(1 for r in records if r.summary_text.startswith(_NEGATIVE_PREFIX)),
    )
    return records
