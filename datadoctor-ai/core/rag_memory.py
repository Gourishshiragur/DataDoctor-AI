"""
RAG Memory Store — semantic incident memory for the DataDoctor AI agent.

Every time the agent successfully diagnoses and fixes a pipeline failure,
that incident (error, diagnosis, fix) is embedded and stored here. On the
next failure, the agent first retrieves the top-k semantically similar past
incidents as context — so it "remembers" how similar problems were solved
before, rather than reasoning from scratch every time.

This is the RAG (Retrieval-Augmented Generation) layer:
  - Documents = past incidents (error + root cause + fix)
  - Retrieval  = semantic similarity via sentence-transformer embeddings
  - Generation = Claude API, now augmented with the retrieved context

Architecture:
  - PRIMARY: ChromaDB (persistent, runs in-process — no server to spin up)
    with sentence-transformers embeddings (all-MiniLM-L6-v2, ~22MB, downloads
    once on first run on Streamlit Cloud)
  - FALLBACK: pure-Python cosine similarity over a JSON store (stdlib + numpy
    only) — used when ChromaDB/sentence-transformers aren't installed, so the
    app degrades gracefully rather than crashing. The fallback is
    architecturally identical: same store/retrieve interface, same data format,
    just TF-IDF-style term-frequency vectors instead of neural embeddings.

Both paths produce: given a query string, return the top-k most similar
past incidents with their root_cause and fix_hint.
"""
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import List, Optional

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CHROMA_DIR = DATA_DIR / "chroma"
FALLBACK_FILE = DATA_DIR / "incident_memory.json"
EMBEDDING_MODEL = "all-MiniLM-L6-v2"


def new_id_short() -> str:
    import uuid
    return uuid.uuid4().hex[:8]


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
    source: str  # "chroma" or "fallback"


# ─────────────────────────────────────────────
# ChromaDB + sentence-transformers path
# ─────────────────────────────────────────────
def _try_chroma():
    """Returns (collection, embedding_fn) or (None, None) if not available."""
    try:
        import chromadb
        from chromadb.utils import embedding_functions
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=EMBEDDING_MODEL
        )
        client = chromadb.PersistentClient(path=str(CHROMA_DIR))
        collection = client.get_or_create_collection(
            name="incident_memory",
            embedding_function=ef,
            metadata={"hnsw:space": "cosine"},
        )
        return collection, ef
    except Exception:
        return None, None


def _incident_text(incident: Incident) -> str:
    """Canonical text representation used for embedding — what gets compared."""
    return (
        f"ERROR: {incident.error_message}\n"
        f"CODE: {incident.step_code[:300]}\n"
        f"ROOT CAUSE: {incident.root_cause}"
    )


def store_incident_chroma(collection, incident: Incident):
    text = _incident_text(incident)
    collection.upsert(
        documents=[text],
        ids=[incident.id],
        metadatas=[{
            "root_cause": incident.root_cause,
            "fix_applied": incident.fix_applied[:500],
            "pipeline_name": incident.pipeline_name,
            "error_message": incident.error_message,
            "confidence": incident.confidence,
            "resolved_at": incident.resolved_at,
        }],
    )


def retrieve_similar_chroma(collection, query: str, k: int = 3) -> List[MemoryResult]:
    count = collection.count()
    if count == 0:
        return []
    results = collection.query(query_texts=[query], n_results=min(k, count))
    out = []
    for i, meta in enumerate(results["metadatas"][0]):
        dist = results["distances"][0][i]
        score = round(1.0 - dist, 3)  # cosine distance → similarity
        out.append(MemoryResult(
            incident=Incident(
                id=results["ids"][0][i],
                error_message=meta.get("error_message", ""),
                step_code="",
                root_cause=meta["root_cause"],
                fix_applied=meta["fix_applied"],
                pipeline_name=meta.get("pipeline_name", ""),
                resolved_at=meta.get("resolved_at", ""),
                confidence=meta.get("confidence", ""),
            ),
            similarity_score=score,
            source="chroma",
        ))
    return out


# ─────────────────────────────────────────────
# Pure-Python cosine-similarity fallback
# ─────────────────────────────────────────────
def _load_fallback() -> List[dict]:
    if not FALLBACK_FILE.exists():
        return []
    return json.loads(FALLBACK_FILE.read_text())


def _save_fallback(incidents: List[dict]):
    FALLBACK_FILE.write_text(json.dumps(incidents, indent=2, default=str))


def _tokenize(text: str) -> dict:
    tokens = re.findall(r"[a-z0-9_]+", text.lower())
    freq: dict = {}
    for t in tokens:
        freq[t] = freq.get(t, 0) + 1
    return freq


def _cosine(a: dict, b: dict) -> float:
    keys = set(a) | set(b)
    dot = sum(a.get(k, 0) * b.get(k, 0) for k in keys)
    mag_a = math.sqrt(sum(v * v for v in a.values()))
    mag_b = math.sqrt(sum(v * v for v in b.values()))
    if mag_a == 0 or mag_b == 0:
        return 0.0
    return dot / (mag_a * mag_b)


def store_incident_fallback(incident: Incident):
    incidents = _load_fallback()
    incidents = [i for i in incidents if i["id"] != incident.id]
    incidents.append({**asdict(incident), "tokens": _tokenize(_incident_text(incident))})
    _save_fallback(incidents)


def retrieve_similar_fallback(query: str, k: int = 3) -> List[MemoryResult]:
    incidents = _load_fallback()
    if not incidents:
        return []
    q_tokens = _tokenize(query)
    scored = [(i, _cosine(q_tokens, i.get("tokens", {}))) for i in incidents]
    scored.sort(key=lambda x: x[1], reverse=True)
    out = []
    for i, score in scored[:k]:
        if score < 0.05:
            continue
        inc = Incident(**{k: v for k, v in i.items() if k != "tokens"})
        out.append(MemoryResult(incident=inc, similarity_score=round(score, 3), source="fallback"))
    return out


# ─────────────────────────────────────────────
# Public API — callers use these two functions only
# ─────────────────────────────────────────────
def store_incident(incident: Incident):
    """Embed and store a resolved incident in whichever vector backend is available."""
    collection, _ = _try_chroma()
    if collection is not None:
        store_incident_chroma(collection, incident)
    else:
        store_incident_fallback(incident)


def retrieve_similar(query: str, k: int = 3) -> List[MemoryResult]:
    """Return the top-k semantically similar past incidents for a given query string."""
    collection, _ = _try_chroma()
    if collection is not None:
        return retrieve_similar_chroma(collection, query, k)
    return retrieve_similar_fallback(query, k)


def incident_count() -> int:
    collection, _ = _try_chroma()
    if collection is not None:
        return collection.count()
    return len(_load_fallback())
