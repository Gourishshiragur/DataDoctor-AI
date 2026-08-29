"""pipeline/lineage.py — records and retrieves data lineage across Bronze -> Silver -> Gold.

In Demo Mode lineage events are logged to database/history.py (SQLite). In Enterprise
Mode, the same log calls are made AND Unity Catalog automatically captures column-level
lineage natively (see dbx_enterprise/unity_catalog.py) — this module just tracks the
pipeline-level "which table fed which table" graph for the UI.
"""
from __future__ import annotations

from database import history


def record(run_id: str, source_layer: str, source_table: str, target_layer: str, target_table: str, transform: str):
    history.log_lineage(run_id, source_layer, source_table, target_layer, target_table, transform)


def graph_edges(run_id: str | None = None) -> list[dict]:
    return history.get_lineage(run_id)


def build_mermaid(run_id: str | None = None) -> str:
    """Render lineage edges as a Mermaid flowchart definition for display in Streamlit."""
    edges = graph_edges(run_id)
    if not edges:
        return "flowchart LR\n  A[No lineage recorded yet]"
    lines = ["flowchart LR"]
    seen_nodes = set()
    for e in edges:
        src = f"{e['source_layer']}_{e['source_table']}"
        tgt = f"{e['target_layer']}_{e['target_table']}"
        for node, label in [(src, f"{e['source_layer'].title()}: {e['source_table']}"),
                             (tgt, f"{e['target_layer'].title()}: {e['target_table']}")]:
            if node not in seen_nodes:
                lines.append(f'  {node}["{label}"]')
                seen_nodes.add(node)
        lines.append(f'  {src} -->|{e["transform"]}| {tgt}')
    return "\n".join(lines)
