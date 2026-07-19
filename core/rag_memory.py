"""
DataDoctor AI — Enterprise RAG Memory

Supports two RAG use cases:

1. Incident Memory RAG
   Stores resolved pipeline incidents:
   - error
   - root cause
   - applied fix
   - pipeline
   - confidence

2. Enterprise Knowledge RAG
   Stores and retrieves:
   - uploaded document text
   - dataset summaries
   - schemas
   - data-quality findings
   - business context
   - analysis results

Primary backend:
- ChromaDB
- SentenceTransformer embeddings

Fallback backend:
- Local JSON persistence
- Pure-Python cosine similarity

No paid API is required.
"""

from __future__ import annotations

import json
import math
import re
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


# ============================================================
# Storage configuration
# ============================================================

DATA_DIR = (
    Path(__file__).resolve().parent.parent
    / "data"
)

DATA_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

CHROMA_DIR = (
    DATA_DIR
    / "chroma"
)

FALLBACK_FILE = (
    DATA_DIR
    / "incident_memory.json"
)

KNOWLEDGE_FILE = (
    DATA_DIR
    / "knowledge_memory.json"
)

EMBEDDING_MODEL = (
    "all-MiniLM-L6-v2"
)


# ============================================================
# Utility functions
# ============================================================

def new_id_short() -> str:
    return uuid.uuid4().hex[:8]


def _safe_text(
    value: Any,
) -> str:
    if value is None:
        return ""

    return str(value).strip()


def _safe_read_json(
    path: Path,
) -> List[dict]:
    """
    Read a JSON list safely.

    A corrupted or partially written local memory file should
    not crash the Streamlit application.
    """

    if not path.exists():
        return []

    try:
        content = path.read_text(
            encoding="utf-8"
        )

        if not content.strip():
            return []

        payload = json.loads(
            content
        )

        if isinstance(
            payload,
            list,
        ):
            return payload

    except Exception:
        return []

    return []


def _safe_write_json(
    path: Path,
    records: List[dict],
) -> None:
    """
    Write JSON through a temporary file to reduce the chance
    of corrupting persistent memory.
    """

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = (
        path.with_suffix(
            f"{path.suffix}.tmp"
        )
    )

    temporary_path.write_text(
        json.dumps(
            records,
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )

    temporary_path.replace(
        path
    )


def _tokenize(
    text: str,
) -> Dict[str, int]:
    """
    Convert text into local term-frequency features.
    """

    tokens = re.findall(
        r"[a-z0-9_]+",
        _safe_text(
            text
        ).lower(),
    )

    frequencies: Dict[
        str,
        int,
    ] = {}

    for token in tokens:
        frequencies[token] = (
            frequencies.get(
                token,
                0,
            )
            + 1
        )

    return frequencies


def _cosine(
    first: Dict[str, int],
    second: Dict[str, int],
) -> float:
    """
    Calculate cosine similarity without an external service.
    """

    keys = (
        set(first)
        | set(second)
    )

    dot_product = sum(
        first.get(
            key,
            0,
        )
        * second.get(
            key,
            0,
        )
        for key in keys
    )

    first_magnitude = math.sqrt(
        sum(
            value * value
            for value
            in first.values()
        )
    )

    second_magnitude = math.sqrt(
        sum(
            value * value
            for value
            in second.values()
        )
    )

    if (
        first_magnitude == 0
        or second_magnitude == 0
    ):
        return 0.0

    return (
        dot_product
        / (
            first_magnitude
            * second_magnitude
        )
    )


# ============================================================
# Incident-memory models
# ============================================================

@dataclass
class Incident:
    id: str
    error_message: str
    step_code: str
    root_cause: str
    fix_applied: str
    pipeline_name: str
    resolved_at: str
    confidence: str


@dataclass
class MemoryResult:
    incident: Incident
    similarity_score: float
    source: str


# ============================================================
# Enterprise knowledge models
# ============================================================

@dataclass
class KnowledgeDocument:
    id: str
    text: str
    source_name: str
    content_type: str
    chunk_index: int
    metadata: Dict[str, Any]


@dataclass
class KnowledgeResult:
    document: KnowledgeDocument
    similarity_score: float
    source: str


# ============================================================
# ChromaDB connection
# ============================================================

def _try_chroma(
    collection_name: str = (
        "incident_memory"
    ),
):
    """
    Return a Chroma collection when local vector search is
    available.

    Return None when:
    - ChromaDB is not installed
    - sentence-transformers is unavailable
    - model download fails
    - storage permission fails

    The application then uses the local JSON fallback.
    """

    try:
        import chromadb

        from chromadb.utils import (
            embedding_functions,
        )

        embedding_function = (
            embedding_functions
            .SentenceTransformerEmbeddingFunction(
                model_name=(
                    EMBEDDING_MODEL
                )
            )
        )

        client = (
            chromadb
            .PersistentClient(
                path=str(
                    CHROMA_DIR
                )
            )
        )

        collection = (
            client
            .get_or_create_collection(
                name=collection_name,
                embedding_function=(
                    embedding_function
                ),
                metadata={
                    "hnsw:space": (
                        "cosine"
                    )
                },
            )
        )

        return (
            collection,
            embedding_function,
        )

    except Exception:
        return (
            None,
            None,
        )


# ============================================================
# Incident Memory RAG
# ============================================================

def _incident_text(
    incident: Incident,
) -> str:
    """
    Canonical incident representation used for retrieval.
    """

    return (
        f"ERROR:\n"
        f"{incident.error_message}\n\n"

        f"PIPELINE:\n"
        f"{incident.pipeline_name}\n\n"

        f"CODE:\n"
        f"{incident.step_code[:1000]}\n\n"

        f"ROOT CAUSE:\n"
        f"{incident.root_cause}\n\n"

        f"FIX:\n"
        f"{incident.fix_applied}"
    )


def store_incident_chroma(
    collection,
    incident: Incident,
) -> None:
    """
    Store or update an incident in ChromaDB.
    """

    collection.upsert(
        documents=[
            _incident_text(
                incident
            )
        ],
        ids=[
            incident.id
        ],
        metadatas=[
            {
                "root_cause": (
                    incident
                    .root_cause
                ),
                "fix_applied": (
                    incident
                    .fix_applied[
                        :4000
                    ]
                ),
                "pipeline_name": (
                    incident
                    .pipeline_name
                ),
                "error_message": (
                    incident
                    .error_message[
                        :2000
                    ]
                ),
                "confidence": (
                    incident
                    .confidence
                ),
                "resolved_at": (
                    incident
                    .resolved_at
                ),
            }
        ],
    )


def retrieve_similar_chroma(
    collection,
    query: str,
    k: int = 3,
) -> List[MemoryResult]:
    """
    Retrieve semantically similar incidents.
    """

    count = collection.count()

    if count == 0:
        return []

    result_limit = max(
        1,
        min(
            int(k),
            count,
        ),
    )

    results = collection.query(
        query_texts=[
            _safe_text(
                query
            )
        ],
        n_results=result_limit,
    )

    output: List[
        MemoryResult
    ] = []

    metadata_rows = (
        results.get(
            "metadatas",
            [[]],
        )[0]
        or []
    )

    identifiers = (
        results.get(
            "ids",
            [[]],
        )[0]
        or []
    )

    distances = (
        results.get(
            "distances",
            [[]],
        )[0]
        or []
    )

    for index, metadata in enumerate(
        metadata_rows
    ):
        distance = (
            distances[index]
            if index
            < len(distances)
            else 1.0
        )

        similarity = max(
            0.0,
            min(
                1.0,
                1.0
                - float(
                    distance
                ),
            ),
        )

        output.append(
            MemoryResult(
                incident=Incident(
                    id=(
                        identifiers[
                            index
                        ]
                        if index
                        < len(
                            identifiers
                        )
                        else (
                            new_id_short()
                        )
                    ),
                    error_message=(
                        metadata.get(
                            "error_message",
                            "",
                        )
                    ),
                    step_code="",
                    root_cause=(
                        metadata.get(
                            "root_cause",
                            "",
                        )
                    ),
                    fix_applied=(
                        metadata.get(
                            "fix_applied",
                            "",
                        )
                    ),
                    pipeline_name=(
                        metadata.get(
                            "pipeline_name",
                            "",
                        )
                    ),
                    resolved_at=(
                        metadata.get(
                            "resolved_at",
                            "",
                        )
                    ),
                    confidence=(
                        metadata.get(
                            "confidence",
                            "",
                        )
                    ),
                ),
                similarity_score=(
                    round(
                        similarity,
                        3,
                    )
                ),
                source="chroma",
            )
        )

    return output


def _load_fallback() -> List[dict]:
    return _safe_read_json(
        FALLBACK_FILE
    )


def _save_fallback(
    incidents: List[dict],
) -> None:
    _safe_write_json(
        FALLBACK_FILE,
        incidents,
    )


def store_incident_fallback(
    incident: Incident,
) -> None:
    """
    Store an incident in local JSON memory.
    """

    incidents = (
        _load_fallback()
    )

    incidents = [
        item
        for item
        in incidents
        if item.get(
            "id"
        )
        != incident.id
    ]

    incidents.append(
        {
            **asdict(
                incident
            ),
            "tokens": (
                _tokenize(
                    _incident_text(
                        incident
                    )
                )
            ),
        }
    )

    _save_fallback(
        incidents
    )


def retrieve_similar_fallback(
    query: str,
    k: int = 3,
) -> List[MemoryResult]:
    """
    Retrieve similar incidents using local cosine similarity.
    """

    incidents = (
        _load_fallback()
    )

    if not incidents:
        return []

    query_tokens = (
        _tokenize(
            query
        )
    )

    scored = []

    for item in incidents:
        item_tokens = (
            item.get(
                "tokens"
            )
            or _tokenize(
                (
                    f"{item.get('error_message', '')} "
                    f"{item.get('root_cause', '')} "
                    f"{item.get('fix_applied', '')}"
                )
            )
        )

        score = _cosine(
            query_tokens,
            item_tokens,
        )

        scored.append(
            (
                item,
                score,
            )
        )

    scored.sort(
        key=lambda value: (
            value[1]
        ),
        reverse=True,
    )

    output: List[
        MemoryResult
    ] = []

    for item, score in (
        scored[
            :max(
                1,
                int(k),
            )
        ]
    ):
        if score < 0.05:
            continue

        incident_data = {
            key: value
            for key, value
            in item.items()
            if key != "tokens"
        }

        output.append(
            MemoryResult(
                incident=Incident(
                    **incident_data
                ),
                similarity_score=(
                    round(
                        float(
                            score
                        ),
                        3,
                    )
                ),
                source="fallback",
            )
        )

    return output


def store_incident(
    incident: Incident,
) -> None:
    """
    Store a resolved incident using the best available local
    vector backend.
    """

    collection, _ = (
        _try_chroma(
            "incident_memory"
        )
    )

    if collection is not None:
        try:
            store_incident_chroma(
                collection,
                incident,
            )

            return

        except Exception:
            pass

    store_incident_fallback(
        incident
    )


def retrieve_similar(
    query: str,
    k: int = 3,
) -> List[MemoryResult]:
    """
    Return top-k similar incidents.

    Existing agent imports remain compatible.
    """

    cleaned_query = (
        _safe_text(
            query
        )
    )

    if not cleaned_query:
        return []

    collection, _ = (
        _try_chroma(
            "incident_memory"
        )
    )

    if collection is not None:
        try:
            return (
                retrieve_similar_chroma(
                    collection,
                    cleaned_query,
                    k,
                )
            )

        except Exception:
            pass

    return (
        retrieve_similar_fallback(
            cleaned_query,
            k,
        )
    )


def incident_count() -> int:
    """
    Return stored incident count.
    """

    collection, _ = (
        _try_chroma(
            "incident_memory"
        )
    )

    if collection is not None:
        try:
            return int(
                collection.count()
            )

        except Exception:
            pass

    return len(
        _load_fallback()
    )


# ============================================================
# Enterprise document/data RAG
# ============================================================

def chunk_text(
    text: Any,
    chunk_size: int = 1200,
    chunk_overlap: int = 180,
) -> List[str]:
    """
    Split enterprise content into overlapping chunks.
    """

    cleaned_text = re.sub(
        r"\n{3,}",
        "\n\n",
        _safe_text(
            text
        ).replace(
            "\x00",
            " ",
        ),
    )

    if not cleaned_text:
        return []

    chunk_size = max(
        300,
        int(
            chunk_size
        ),
    )

    chunk_overlap = max(
        0,
        min(
            int(
                chunk_overlap
            ),
            chunk_size
            // 2,
        ),
    )

    if (
        len(
            cleaned_text
        )
        <= chunk_size
    ):
        return [
            cleaned_text
        ]

    chunks: List[str] = []

    start = 0

    while (
        start
        < len(
            cleaned_text
        )
    ):
        end = min(
            start
            + chunk_size,
            len(
                cleaned_text
            ),
        )

        chunk = (
            cleaned_text[
                start:end
            ]
        )

        if (
            end
            < len(
                cleaned_text
            )
        ):
            break_positions = [
                chunk.rfind(
                    "\n\n"
                ),
                chunk.rfind(
                    "\n"
                ),
                chunk.rfind(
                    ". "
                ),
                chunk.rfind(
                    " "
                ),
            ]

            best_break = max(
                break_positions
            )

            if (
                best_break
                > int(
                    chunk_size
                    * 0.55
                )
            ):
                end = (
                    start
                    + best_break
                    + 1
                )

                chunk = (
                    cleaned_text[
                        start:end
                    ]
                )

        chunk = (
            chunk.strip()
        )

        if chunk:
            chunks.append(
                chunk
            )

        if (
            end
            >= len(
                cleaned_text
            )
        ):
            break

        next_start = (
            end
            - chunk_overlap
        )

        if (
            next_start
            <= start
        ):
            next_start = end

        start = next_start

    return chunks


def _knowledge_id(
    source_name: str,
    chunk_index: int,
    text: str,
) -> str:
    """
    Generate a stable ID so re-indexing the same content does
    not create duplicate records.
    """

    raw_value = (
        f"{source_name}|"
        f"{chunk_index}|"
        f"{text}"
    )

    import hashlib

    return (
        hashlib.sha256(
            raw_value.encode(
                "utf-8",
                errors="ignore",
            )
        )
        .hexdigest()[
            :24
        ]
    )


def _load_knowledge() -> List[dict]:
    return _safe_read_json(
        KNOWLEDGE_FILE
    )


def _save_knowledge(
    records: List[dict],
) -> None:
    _safe_write_json(
        KNOWLEDGE_FILE,
        records,
    )


def add_knowledge(
    text: Any,
    source_name: str = (
        "Uploaded content"
    ),
    content_type: str = (
        "document"
    ),
    metadata: Optional[
        Dict[str, Any]
    ] = None,
    chunk_size: int = 1200,
    chunk_overlap: int = 180,
) -> int:
    """
    Add document, dataset, schema, quality, or business
    context to enterprise RAG.

    Returns:
        Number of new or updated chunks.
    """

    source_name = (
        _safe_text(
            source_name
        )
        or "Uploaded content"
    )

    content_type = (
        _safe_text(
            content_type
        )
        or "document"
    )

    chunks = chunk_text(
        text=text,
        chunk_size=chunk_size,
        chunk_overlap=(
            chunk_overlap
        ),
    )

    if not chunks:
        return 0

    base_metadata = dict(
        metadata
        or {}
    )

    collection, _ = (
        _try_chroma(
            "enterprise_knowledge"
        )
    )

    records = (
        _load_knowledge()
    )

    record_map = {
        item.get(
            "id"
        ): item
        for item
        in records
        if item.get(
            "id"
        )
    }

    added_count = 0

    for index, chunk in (
        enumerate(
            chunks
        )
    ):
        document_id = (
            _knowledge_id(
                source_name,
                index,
                chunk,
            )
        )

        document = (
            KnowledgeDocument(
                id=document_id,
                text=chunk,
                source_name=(
                    source_name
                ),
                content_type=(
                    content_type
                ),
                chunk_index=(
                    index
                ),
                metadata={
                    **base_metadata,
                    "source_name": (
                        source_name
                    ),
                    "content_type": (
                        content_type
                    ),
                    "chunk_index": (
                        index
                    ),
                },
            )
        )

        local_record = {
            **asdict(
                document
            ),
            "tokens": (
                _tokenize(
                    chunk
                )
            ),
        }

        record_map[
            document_id
        ] = local_record

        if (
            collection
            is not None
        ):
            try:
                chroma_metadata = {
                    "source_name": (
                        source_name
                    ),
                    "content_type": (
                        content_type
                    ),
                    "chunk_index": (
                        int(
                            index
                        )
                    ),
                    "metadata_json": (
                        json.dumps(
                            base_metadata,
                            default=str,
                        )[
                            :4000
                        ]
                    ),
                }

                collection.upsert(
                    documents=[
                        chunk
                    ],
                    ids=[
                        document_id
                    ],
                    metadatas=[
                        chroma_metadata
                    ],
                )

            except Exception:
                pass

        added_count += 1

    _save_knowledge(
        list(
            record_map.values()
        )
    )

    return added_count


def add_dataset_context(
    dataframe,
    source_name: str = (
        "Uploaded dataset"
    ),
    max_sample_rows: int = 200,
) -> int:
    """
    Add dataset structure and a bounded sample to RAG.

    Exact calculations should still be performed directly on
    the DataFrame. RAG supplies relevant context and evidence.
    """

    if dataframe is None:
        return 0

    try:
        row_count = len(
            dataframe
        )

        column_count = len(
            dataframe.columns
        )

    except Exception:
        return 0

    lines = [
        (
            f"Dataset source: "
            f"{source_name}"
        ),
        (
            f"Total rows: "
            f"{row_count}"
        ),
        (
            f"Total columns: "
            f"{column_count}"
        ),
        "",
        "Schema:",
    ]

    for column in (
        dataframe.columns
    ):
        try:
            dtype = (
                dataframe[
                    column
                ].dtype
            )

        except Exception:
            dtype = "unknown"

        lines.append(
            f"- {column}: "
            f"{dtype}"
        )

    try:
        missing = (
            dataframe
            .isna()
            .sum()
        )

        lines.extend(
            [
                "",
                "Missing values:",
            ]
        )

        for (
            column,
            count,
        ) in (
            missing.items()
        ):
            lines.append(
                f"- {column}: "
                f"{int(count)}"
            )

    except Exception:
        pass

    try:
        duplicate_count = int(
            dataframe
            .duplicated()
            .sum()
        )

        lines.extend(
            [
                "",
                (
                    "Duplicate rows: "
                    f"{duplicate_count}"
                ),
            ]
        )

    except Exception:
        pass

    try:
        numeric_columns = (
            dataframe
            .select_dtypes(
                include="number"
            )
            .columns
            .tolist()
        )

        if numeric_columns:
            summary = (
                dataframe[
                    numeric_columns
                ]
                .describe()
                .transpose()
                .to_string()
            )

            lines.extend(
                [
                    "",
                    "Numeric summary:",
                    summary,
                ]
            )

    except Exception:
        pass

    try:
        sample_size = min(
            max(
                0,
                int(
                    max_sample_rows
                ),
            ),
            row_count,
        )

        if sample_size:
            sample = (
                dataframe
                .head(
                    sample_size
                )
                .fillna("")
                .astype(str)
                .to_dict(
                    orient="records"
                )
            )

            lines.extend(
                [
                    "",
                    "Dataset sample:",
                ]
            )

            for (
                row_number,
                row,
            ) in enumerate(
                sample,
                start=1,
            ):
                row_text = (
                    " | ".join(
                        (
                            f"{key}="
                            f"{value}"
                        )
                        for (
                            key,
                            value,
                        )
                        in row.items()
                    )
                )

                lines.append(
                    (
                        f"Row "
                        f"{row_number}: "
                        f"{row_text}"
                    )
                )

    except Exception:
        pass

    return add_knowledge(
        text="\n".join(
            lines
        ),
        source_name=(
            source_name
        ),
        content_type=(
            "dataset"
        ),
        metadata={
            "rows": (
                row_count
            ),
            "columns": (
                column_count
            ),
        },
    )


def retrieve_knowledge(
    query: str,
    k: int = 5,
    minimum_score: float = 0.03,
) -> List[KnowledgeResult]:
    """
    Retrieve enterprise knowledge relevant to a question.
    """

    cleaned_query = (
        _safe_text(
            query
        )
    )

    if not cleaned_query:
        return []

    result_limit = max(
        1,
        min(
            int(
                k
            ),
            20,
        ),
    )

    collection, _ = (
        _try_chroma(
            "enterprise_knowledge"
        )
    )

    if collection is not None:
        try:
            count = int(
                collection.count()
            )

            if count > 0:
                results = (
                    collection.query(
                        query_texts=[
                            cleaned_query
                        ],
                        n_results=min(
                            result_limit,
                            count,
                        ),
                    )
                )

                documents = (
                    results.get(
                        "documents",
                        [[]],
                    )[0]
                    or []
                )

                identifiers = (
                    results.get(
                        "ids",
                        [[]],
                    )[0]
                    or []
                )

                metadatas = (
                    results.get(
                        "metadatas",
                        [[]],
                    )[0]
                    or []
                )

                distances = (
                    results.get(
                        "distances",
                        [[]],
                    )[0]
                    or []
                )

                output = []

                for index, text in (
                    enumerate(
                        documents
                    )
                ):
                    metadata = (
                        metadatas[
                            index
                        ]
                        if index
                        < len(
                            metadatas
                        )
                        else {}
                    )

                    distance = (
                        distances[
                            index
                        ]
                        if index
                        < len(
                            distances
                        )
                        else 1.0
                    )

                    score = max(
                        0.0,
                        min(
                            1.0,
                            1.0
                            - float(
                                distance
                            ),
                        ),
                    )

                    if (
                        score
                        < minimum_score
                    ):
                        continue

                    extra_metadata = {}

                    try:
                        extra_metadata = (
                            json.loads(
                                metadata.get(
                                    "metadata_json",
                                    "{}",
                                )
                            )
                        )

                    except Exception:
                        extra_metadata = {}

                    output.append(
                        KnowledgeResult(
                            document=(
                                KnowledgeDocument(
                                    id=(
                                        identifiers[
                                            index
                                        ]
                                        if index
                                        < len(
                                            identifiers
                                        )
                                        else (
                                            new_id_short()
                                        )
                                    ),
                                    text=(
                                        text
                                    ),
                                    source_name=(
                                        metadata.get(
                                            "source_name",
                                            (
                                                "Uploaded "
                                                "content"
                                            ),
                                        )
                                    ),
                                    content_type=(
                                        metadata.get(
                                            "content_type",
                                            "document",
                                        )
                                    ),
                                    chunk_index=int(
                                        metadata.get(
                                            "chunk_index",
                                            index,
                                        )
                                    ),
                                    metadata=(
                                        extra_metadata
                                    ),
                                )
                            ),
                            similarity_score=(
                                round(
                                    score,
                                    4,
                                )
                            ),
                            source="chroma",
                        )
                    )

                if output:
                    return output

        except Exception:
            pass

    records = (
        _load_knowledge()
    )

    if not records:
        return []

    query_tokens = (
        _tokenize(
            cleaned_query
        )
    )

    scored_records = []

    for record in records:
        tokens = (
            record.get(
                "tokens"
            )
            or _tokenize(
                record.get(
                    "text",
                    "",
                )
            )
        )

        score = _cosine(
            query_tokens,
            tokens,
        )

        scored_records.append(
            (
                record,
                score,
            )
        )

    scored_records.sort(
        key=lambda item: (
            item[1]
        ),
        reverse=True,
    )

    output: List[
        KnowledgeResult
    ] = []

    for (
        record,
        score,
    ) in (
        scored_records[
            :result_limit
        ]
    ):
        if (
            score
            < minimum_score
        ):
            continue

        output.append(
            KnowledgeResult(
                document=(
                    KnowledgeDocument(
                        id=(
                            record.get(
                                "id",
                                (
                                    new_id_short()
                                ),
                            )
                        ),
                        text=(
                            record.get(
                                "text",
                                "",
                            )
                        ),
                        source_name=(
                            record.get(
                                "source_name",
                                (
                                    "Uploaded "
                                    "content"
                                ),
                            )
                        ),
                        content_type=(
                            record.get(
                                "content_type",
                                "document",
                            )
                        ),
                        chunk_index=int(
                            record.get(
                                "chunk_index",
                                0,
                            )
                        ),
                        metadata=(
                            record.get(
                                "metadata",
                                {},
                            )
                        ),
                    )
                ),
                similarity_score=(
                    round(
                        float(
                            score
                        ),
                        4,
                    )
                ),
                source="fallback",
            )
        )

    return output


def get_rag_context(
    query: str,
    k: int = 5,
    max_characters: int = 8000,
) -> Dict[str, Any]:
    """
    Return source-aware context for the AI assistant.

    Output:
    {
        "context": "...",
        "sources": [...],
        "matches": [...],
        "retrieved": True
    }
    """

    matches = (
        retrieve_knowledge(
            query=query,
            k=k,
        )
    )

    if not matches:
        return {
            "context": "",
            "sources": [],
            "matches": [],
            "retrieved": False,
        }

    context_blocks = []

    sources = []

    match_payload = []

    used_characters = 0

    for (
        position,
        result,
    ) in enumerate(
        matches,
        start=1,
    ):
        document = (
            result.document
        )

        if (
            document.source_name
            not in sources
        ):
            sources.append(
                document
                .source_name
            )

        block = (
            f"[Source {position}: "
            f"{document.source_name}]\n"
            f"{document.text}"
        )

        remaining = (
            max(
                0,
                int(
                    max_characters
                ),
            )
            - used_characters
        )

        if remaining <= 0:
            break

        if (
            len(
                block
            )
            > remaining
        ):
            block = (
                block[
                    :remaining
                ]
            )

        context_blocks.append(
            block
        )

        used_characters += (
            len(
                block
            )
        )

        match_payload.append(
            {
                "source": (
                    document
                    .source_name
                ),
                "content_type": (
                    document
                    .content_type
                ),
                "chunk_index": (
                    document
                    .chunk_index
                ),
                "score": (
                    result
                    .similarity_score
                ),
                "backend": (
                    result.source
                ),
            }
        )

    return {
        "context": (
            "\n\n".join(
                context_blocks
            )
        ),
        "sources": sources,
        "matches": (
            match_payload
        ),
        "retrieved": bool(
            context_blocks
        ),
    }


def knowledge_count() -> int:
    """
    Return enterprise knowledge chunk count.
    """

    collection, _ = (
        _try_chroma(
            "enterprise_knowledge"
        )
    )

    if collection is not None:
        try:
            return int(
                collection.count()
            )

        except Exception:
            pass

    return len(
        _load_knowledge()
    )


def clear_knowledge() -> None:
    """
    Clear enterprise document/data RAG memory.

    Incident memory is intentionally preserved.
    """

    if (
        KNOWLEDGE_FILE
        .exists()
    ):
        try:
            KNOWLEDGE_FILE.unlink()

        except Exception:
            _save_knowledge(
                []
            )

    try:
        import chromadb

        client = (
            chromadb
            .PersistentClient(
                path=str(
                    CHROMA_DIR
                )
            )
        )

        try:
            client.delete_collection(
                name=(
                    "enterprise_knowledge"
                )
            )

        except Exception:
            pass

    except Exception:
        pass


# ============================================================
# Compatibility aliases for Chat/Analytics integration
# ============================================================

def add_text(
    text: Any,
    source: str = (
        "Uploaded content"
    ),
    metadata: Optional[
        Dict[str, Any]
    ] = None,
) -> int:
    return add_knowledge(
        text=text,
        source_name=source,
        content_type=(
            "document"
        ),
        metadata=metadata,
    )


def add_document(
    text: Any,
    source: str = (
        "Uploaded content"
    ),
    metadata: Optional[
        Dict[str, Any]
    ] = None,
) -> int:
    return add_text(
        text=text,
        source=source,
        metadata=metadata,
    )


def add_to_memory(
    text: Any,
    source: str = (
        "Uploaded content"
    ),
    metadata: Optional[
        Dict[str, Any]
    ] = None,
) -> int:
    return add_text(
        text=text,
        source=source,
        metadata=metadata,
    )


def retrieve_context(
    query: str,
    top_k: int = 5,
) -> Dict[str, Any]:
    return get_rag_context(
        query=query,
        k=top_k,
    )


def get_context(
    query: str,
    top_k: int = 5,
) -> str:
    return (
        get_rag_context(
            query=query,
            k=top_k,
        )
        .get(
            "context",
            "",
        )
    )


def memory_stats() -> Dict[str, Any]:
    """
    Combined health information for UI and monitoring.
    """

    return {
        "incident_count": (
            incident_count()
        ),
        "knowledge_chunks": (
            knowledge_count()
        ),
        "embedding_model": (
            EMBEDDING_MODEL
        ),
        "primary_backend": (
            "ChromaDB"
        ),
        "fallback_backend": (
            "Local JSON cosine search"
        ),
        "paid_api_required": (
            False
        ),
    }