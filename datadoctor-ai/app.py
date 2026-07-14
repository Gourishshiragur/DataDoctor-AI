import streamlit as st

from core import store
from core.ui import inject_global_css, sidebar_brand, status_badge

st.set_page_config(page_title="DataDoctor AI", page_icon="🩺", layout="wide")
inject_global_css()
sidebar_brand()

st.sidebar.page_link("app.py", label="🏠 Dashboard")
st.sidebar.page_link("pages/1_Pipeline_Builder.py", label="🛠️ Pipeline Builder")
st.sidebar.page_link("pages/2_Monitor_and_Repair.py", label="📊 Monitor & Repair")
st.sidebar.page_link("pages/6_Analytics.py", label="📈 Analytics")
st.sidebar.page_link("pages/3_AI_Code_Assistant.py", label="🤖 AI Code Assistant")
st.sidebar.page_link("pages/5_AI_Agent.py", label="🧠 AI Agent Console")
st.sidebar.page_link("pages/7_Chat.py", label="💬 Conversational AI")
st.sidebar.page_link("pages/4_Logs.py", label="📜 Logs")

st.title("🩺 DataDoctor AI")
st.caption("Enterprise-style console for building, monitoring, repairing, and debugging Lakehouse pipelines.")

pipelines = store.load_pipelines()
runs = store.load_runs()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Pipelines", len(pipelines))
col2.metric("Total Runs", len(runs))
col3.metric("Failed Runs", sum(1 for r in runs if r["status"] == "FAILED"))
succeeded = sum(1 for r in runs if r["status"] == "SUCCEEDED")
col4.metric("Success Rate", f"{(succeeded / len(runs) * 100):.0f}%" if runs else "—")

st.divider()

left, right = st.columns([3, 2])

with left:
    st.subheader("Recent Runs")
    if not runs:
        st.info("No runs yet. Create a pipeline and run it from **Pipeline Builder**.")
    else:
        for run in sorted(runs, key=lambda r: r["started_at"], reverse=True)[:8]:
            st.markdown(
                f"""<div class="ddai-card">
                <div class="ddai-title">{run['pipeline_name']} {status_badge(run['status'])}</div>
                <div class="ddai-subtle">run {run['id']} · started {run['started_at'][:19].replace('T',' ')}</div>
                </div>""",
                unsafe_allow_html=True,
            )

with right:
    st.subheader("Pipelines")
    if not pipelines:
        st.info("No pipelines yet.")
    else:
        for p in pipelines:
            st.markdown(
                f"""<div class="ddai-card">
                <div class="ddai-title">{p['name']}</div>
                <div class="ddai-subtle">{len(p['steps'])} steps · {p.get('description','')}</div>
                </div>""",
                unsafe_allow_html=True,
            )

st.divider()
st.caption(
    "DataDoctor AI simulates pipeline execution against realistic failure modes to demonstrate "
    "orchestration, retry, and repair mechanics without requiring a live Spark cluster. "
    "See README for how to point it at a real Databricks Jobs backend."
)
