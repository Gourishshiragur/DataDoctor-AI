"""
rag/retriever.py
-----------------
Lightweight RAG (retrieval-augmented generation) over the app's own knowledge base:
- docs/*.md (architecture, dataset descriptions)
- past run summaries/quality-check results from database/history.py

No vector DB needed for this scale — uses simple TF-IDF style keyword scoring
(via scikit-learn-free pure-Python overlap scoring) to keep the footprint minimal.
The Business AI page uses this to ground answers in the app's own documentation
when a user asks "why" something happened, not just "what" the numbers are.
"""
from __future__ import annotations

import re
from pathlib import Path

from database import history

ROOT_DIR = Path(__file__).resolve().parent.parent
DOCS_DIR = ROOT_DIR / "docs"


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def _load_doc_chunks() -> list[dict]:
    chunks = []
    if DOCS_DIR.exists():
        for f in DOCS_DIR.glob("*.md"):
            text = f.read_text(errors="ignore")
            for para in text.split("\n\n"):
                if len(para.strip()) > 40:
                    chunks.append({"source": f.name, "text": para.strip()})
    return chunks


def _load_run_chunks(limit: int = 20) -> list[dict]:
    chunks = []
    for run in history.get_runs(limit=limit):
        for r in history.get_repairs(run["run_id"]):
            chunks.append({
                "source": f"run:{run['run_id']}",
                "text": f"Dataset {run['dataset']}: {r['column_name']} had {r['issue']}, fixed via {r['action']}.",
            })
    return chunks


def retrieve(query: str, top_k: int = 5) -> list[dict]:
    q_tokens = _tokenize(query)
    corpus = _load_doc_chunks() + _load_run_chunks()
    scored = []
    for c in corpus:
        overlap = len(q_tokens & _tokenize(c["text"]))
        if overlap > 0:
            scored.append((overlap, c))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored[:top_k]]
