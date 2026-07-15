"""
DataDoctor AI — Dashboard
Enterprise overview: pipeline health, AI status, recent activity.
"""
import pandas as pd
import streamlit as st

from core import store
from core.llm_provider import llm_status
from core.rag_memory import incident_count, knowledge_count
from core.ui import inject_global_css, sidebar_brand, status_badge

inject_global_css()
sidebar_brand()

st.title("🩺 DataDoctor AI")
st.caption("Enterprise Lakehouse pipeline operations, observability, AI-assisted diagnosis, and RAG-powered knowledge.")

# ── AI & RAG status banner ─────────────────────────────────────────────────
status = llm_status()
tier_icons = {1: "🟢", 2: "🟡", 3: "🔴"}
tier_icon = tier_icons.get(status["tier"], "⚪")

col_ai, col_rag1, col_rag2 = st.columns(3)
with col_ai:
    st.metric(
        label=f"{tier_icon} AI Provider",
        value=status["provider"],
        help=status["note"],
    )
with col_rag1:
    st.metric("🗄️ Incident Memory", f"{incident_count()} incidents", help="RAG: past resolved pipeline failures")
with col_rag2:
    st.metric("📚 Knowledge Base", f"{knowledge_count()} chunks", help="RAG: uploaded docs, schemas, runbooks")

if status["tier"] == 3:
    st.warning(
        "⚠️ No LLM configured — AI features use rule-based fallback only. "
        "Add **ANTHROPIC_API_KEY** in Streamlit → ⋮ → Settings → Secrets for live Claude AI, "
        "or run Ollama locally for free offline LLM support. "
        "All pipeline, monitoring, repair, and RAG features work without any LLM.",
        icon=None,
    )
elif status["tier"] == 2:
    st.info(f"🟡 Using local Ollama — model: `{status['model']}`. Works offline, not available on Streamlit Cloud.")
else:
    st.success(f"🟢 Claude API active — model: `{status['model']}`. Full AI features enabled.")

st.divider()

# ── Pipeline metrics ────────────────────────────────────────────────────────
pipelines = store.load_pipelines()
runs = store.load_runs()
succeeded = sum(r["status"] == "SUCCEEDED" for r in runs)
failed = sum(r["status"] == "FAILED" for r in runs)
repaired = sum(r["status"] in ("PARTIALLY_REPAIRED",) for r in runs)
success_rate = f"{succeeded / len(runs) * 100:.0f}%" if runs else "—"

m1, m2, m3, m4, m5 = st.columns(5)
m1.metric("Pipelines", len(pipelines))
m2.metric("Total Runs", len(runs))
m3.metric("Succeeded", succeeded)
m4.metric("Failed", failed)
m5.metric("Success Rate", success_rate)

st.divider()

left, right = st.columns([3, 2])

with left:
    st.subheader("Recent pipeline runs")
    if runs:
        rows = []
        for r in sorted(runs, key=lambda x: x["started_at"], reverse=True)[:20]:
            total_rows = sum(x.get("rows_processed") or 0 for x in r.get("step_runs", []))
            steps_ok = sum(1 for x in r.get("step_runs", []) if x["status"] == "SUCCEEDED")
            steps_total = len(r.get("step_runs", []))
            rows.append({
                "Pipeline": r["pipeline_name"],
                "Status": r["status"],
                "Steps": f"{steps_ok}/{steps_total}",
                "Rows": f"{total_rows:,}" if total_rows else "—",
                "Started": r["started_at"][:19].replace("T", " "),
                "Run ID": r["id"],
            })
        df = pd.DataFrame(rows)

        def color_status(val):
            colors = {
                "SUCCEEDED": "color: #3FB950",
                "FAILED": "color: #F85149",
                "RUNNING": "color: #5B8DEF",
                "PARTIALLY_REPAIRED": "color: #D29922",
            }
            return colors.get(val, "")

        st.dataframe(
            df.style.applymap(color_status, subset=["Status"]),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No runs yet. Go to **Pipeline Builder**, pick a template, save, and run it.")

with right:
    st.subheader("Registered pipelines")
    if pipelines:
        for p in pipelines:
            with st.container(border=True):
                st.markdown(f"**{p['name']}**")
                if p.get("description"):
                    st.caption(p["description"])
                step_types = [s.get("step_type", "?") for s in p.get("steps", [])]
                st.caption(f"{len(p.get('steps', []))} steps — {' → '.join(step_types)}")
    else:
        st.info("No pipelines yet.")

st.divider()
st.caption(
    "Pipeline execution is simulated for zero-cost deployment. "
    "Orchestration, DAG dependencies, retries, repair, logs, analytics, RAG memory, "
    "and AI agent workflows are all functional. "
    "To connect to a real Databricks backend, replace `_execute_step()` in "
    "`core/pipeline_engine.py` with a Databricks Jobs API call."
)
