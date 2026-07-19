import pandas as pd
import streamlit as st

from core import store
from core.ui import (
    inject_global_css,
    sidebar_brand,
    status_badge,
    execution_backend_meta,
    execution_backend_badge,
    ai_provider_status,
)

try:
    from core.rag_memory import incident_count, knowledge_count
except Exception:
    def incident_count(): return 0
    def knowledge_count(): return 0

try:
    from core.databricks_executor import configured as databricks_configured
except Exception:
    def databricks_configured(): return False

inject_global_css()
sidebar_brand()

st.title("🩺 DataDoctor AI")
st.caption("Enterprise pipeline operations, observability, repair, analytics, and AI-assisted diagnosis.")

pipelines, runs = store.load_pipelines(), store.load_runs()
succeeded = sum(r["status"] == "SUCCEEDED" for r in runs)
failed = sum(r["status"] == "FAILED" for r in runs)
running = sum(r["status"] == "RUNNING" for r in runs)
repaired = sum(r["status"] in ("REPAIRED", "PARTIALLY_REPAIRED") for r in runs)

# ---------------------------------------------------------------
# Row 1 — headline numbers. Every value below is computed from the
# SAME `runs` list, so these can never contradict each other again.
# ---------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.metric("Pipelines", len(pipelines))
c2.metric("Total Runs", len(runs))
c3.metric("Failed Runs", failed)
c4.metric("Success Rate", f"{succeeded/len(runs)*100:.0f}%" if runs else "—")

# ---------------------------------------------------------------
# Row 2 — operational depth: running now, incidents remembered,
# repair outcomes, and a transparent health score with its formula
# shown up front (no mystery number).
# ---------------------------------------------------------------
d1, d2, d3, d4 = st.columns(4)
d1.metric("Running Now", running, help="Runs currently in progress (this app executes synchronously, so this is usually 0).")
d2.metric("Incidents Remembered", incident_count(), help="Resolved failures stored in RAG incident memory for future diagnosis.")
d3.metric("Auto-Repaired Runs", repaired, help="Runs that recovered via retry/repair rather than a clean first pass.")

if runs:
    health_score = round(
        (succeeded / len(runs)) * 70
        + (repaired / len(runs)) * 20
        + (min(incident_count(), 10) / 10) * 10
    )
else:
    health_score = None
d4.metric(
    "Health Score",
    f"{health_score}/100" if health_score is not None else "—",
    help="70% success rate + 20% successful auto-repairs + 10% incident-memory coverage (capped at 10 incidents). Formula, not a black box.",
)

st.divider()

# ---------------------------------------------------------------
# Execution mode mix — the honest replacement for fake "ADF Jobs /
# Databricks Jobs" counters. This shows exactly how each run was
# actually executed: Databricks, verified local data, or simulation.
# ---------------------------------------------------------------
st.subheader("Execution mode")
mode_counts = {}
for r in runs:
    backend = r.get("execution_backend", "local-orchestration-simulation")
    mode_counts[backend] = mode_counts.get(backend, 0) + 1

if mode_counts:
    cols = st.columns(len(mode_counts) + 1)
    for col, (backend, count) in zip(cols, mode_counts.items()):
        meta = execution_backend_meta(backend)
        col.markdown(execution_backend_badge(backend), unsafe_allow_html=True)
        col.metric(" ", count, label_visibility="collapsed")
        col.caption(meta["note"])
    cols[-1].markdown("**Databricks configured?**")
    cols[-1].markdown("🟢 Yes — real jobs will run" if databricks_configured() else "🟠 No — set `DATABRICKS_HOST` / `DATABRICKS_TOKEN` in secrets to enable real execution")
else:
    st.info("No runs yet, so there's no execution mode to report. Run a pipeline in Pipeline Builder to populate this.")

st.caption(
    "Databricks configured: " + ("✅ yes" if databricks_configured() else "❌ no")
    + " · AI provider: " + ai_provider_status()["provider"]
    + " · Knowledge base chunks: " + str(knowledge_count())
)

st.divider()
st.subheader("Recent pipeline runs")
if runs:
    rows = []
    for r in sorted(runs, key=lambda x: x["started_at"], reverse=True)[:20]:
        backend = r.get("execution_backend", "local-orchestration-simulation")
        rows.append({
            "Run ID": r["id"],
            "Pipeline": r["pipeline_name"],
            "Status": r["status"],
            "Backend": execution_backend_meta(backend)["label"],
            "Started": r["started_at"][:19].replace("T", " "),
            "Steps": len(r.get("step_runs", [])),
            "Rows processed": sum(x.get("rows_processed") or 0 for x in r.get("step_runs", [])),
        })
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.info("No runs yet. Open Pipeline Builder, save a template, and run it.")

st.subheader("Registered pipelines")
if pipelines:
    st.dataframe(
        pd.DataFrame([{
            "Pipeline": p["name"],
            "Description": p.get("description", ""),
            "Steps": len(p.get("steps", [])),
            "Created": p.get("created_at", "")[:19].replace("T", " "),
        } for p in pipelines]),
        use_container_width=True,
        hide_index=True,
    )
else:
    st.info("No pipelines created yet.")

st.caption(
    "Execution mode is per-run and shown above: Databricks (real PySpark) when configured, "
    "verified-local when you upload a real file, or simulation otherwise. "
    "Orchestration, dependencies, retries, repair, logs, analytics, and agent workflows are functional in all three modes."
)
