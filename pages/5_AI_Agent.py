import streamlit as st

from core import store
from core.agent import apply_fix, diagnose, diagnose_stream, record_resolved_incident
from core.rag_memory import incident_count, retrieve_similar
from core.retry_engine import retry_step
from core.ui import inject_global_css, sidebar_brand, status_badge

inject_global_css()
sidebar_brand()

st.title("🧠 AI Agent Console")
st.caption(
    "Observe → reason → act. The agent retrieves similar past incidents (RAG), "
    "calls tools to explain errors and suggest optimizations, then proposes a fix. "
    "Every action requires your confirmation — nothing changes without a click."
)

# RAG memory stats
st.info("🧠 Agent workflow: select a failed step → diagnose → review the proposed fix → apply, retry, and learn. RAG memory is loaded only when diagnosis is requested so the page stays responsive.")

runs = sorted(store.load_runs(), key=lambda r: r["started_at"], reverse=True)
pipelines_by_id = {p["id"]: p for p in store.load_pipelines()}

failed_steps = []
for run in runs:
    pipeline = pipelines_by_id.get(run["pipeline_id"])
    if not pipeline:
        continue
    for sr in run["step_runs"]:
        if sr["status"] == "FAILED":
            step = next((s for s in pipeline["steps"] if s["id"] == sr["step_id"]), None)
            if step:
                failed_steps.append((run, pipeline, step, sr))

if not failed_steps:
    st.warning("No failed steps are available for diagnosis yet.")
    st.markdown("""
**What this module does**

1. **Observe** — reads the failed step code, error, and execution logs.
2. **Reason** — retrieves similar incidents and uses the configured LLM when available; otherwise it uses the zero-cost local diagnostic engine.
3. **Act** — proposes corrected code. Nothing changes until you confirm.
4. **Retry & learn** — applies the approved fix, retries the step, and stores the incident for future retrieval.

To test it: open **Pipeline Builder**, run **Idempotent micro-batch upsert** several times until a step fails, then return here.
""")
    st.stop()

labels = [
    f"{run['pipeline_name']} · {step['name']} · run {run['id']}"
    for run, pipeline, step, sr in failed_steps
]
selected_idx = st.selectbox("Select failed step to diagnose", range(len(labels)), format_func=lambda i: labels[i])
run, pipeline, step, step_run = failed_steps[selected_idx]

col1, col2 = st.columns(2)
with col1:
    st.markdown(f"**Step:** {step['name']} &nbsp; {status_badge(step_run['status'])}", unsafe_allow_html=True)
    st.code(step["code"], language="python" if step["engine"] == "pyspark" else "sql")
with col2:
    st.markdown("**Error**")
    st.error(step_run.get("error_message") or "unknown error")
    logs = [l for l in store.load_logs(run["id"]) if l["step_id"] == step["id"]]
    with st.expander(f"Step logs ({len(logs)})"):
        for l in logs[-10:]:
            color = {"ERROR": "red", "WARN": "orange", "INFO": "gray"}.get(l["level"], "gray")
            st.markdown(f":{color}[{l['level']}] {l['message']}")

# RAG retrieval is performed inside the diagnosis workflow, not during page load.

st.divider()
use_streaming = st.toggle("Stream response token-by-token", value=True)
if st.button("🔍 Diagnose with agent", type="primary"):
    st.session_state.pop("last_diagnosis", None)

    if use_streaming:
        st.markdown("**Agent reasoning (streaming):**")
        response_box = st.empty()
        full_text = ""
        diagnosis = None
        with st.spinner("Agent connecting..."):
            for chunk in diagnose_stream(run, pipeline, step["id"]):
                if isinstance(chunk, dict):
                    diagnosis = chunk["diagnosis"]
                else:
                    full_text += chunk
                    response_box.markdown(full_text + "▌")
        response_box.markdown(full_text)
        if diagnosis:
            st.session_state["last_diagnosis"] = diagnosis
    else:
        with st.spinner("Agent observing logs + reasoning..."):
            diagnosis = diagnose(run, pipeline, step["id"])
        st.session_state["last_diagnosis"] = diagnosis

diagnosis = st.session_state.get("last_diagnosis")
if diagnosis:
    st.divider()
    conf_color = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(diagnosis.confidence, "⚪")
    st.markdown(f"### Diagnosis {conf_color} confidence: `{diagnosis.confidence}`")

    if diagnosis.similar_incidents:
        st.caption(f"📚 RAG retrieved {len(diagnosis.similar_incidents)} similar past incident(s) as context")
    if diagnosis.tools_used:
        st.caption(f"🔧 Tools called: {', '.join(set(diagnosis.tools_used))}")

    src_label = {
        "claude-api": "✅ Claude API (tool-augmented)",
        "rule-based": "🟢 Zero-cost local diagnostic engine",
        "streaming": "✅ Claude API (streaming)",
    }.get(diagnosis.source, diagnosis.source)
    st.caption(src_label)

    st.write(f"**Root cause:** {diagnosis.root_cause}")
    st.markdown("**Proposed fix:**")
    lang = "python" if step["engine"] == "pyspark" else "sql"
    st.code(diagnosis.fixed_code, language=lang)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("✅ Apply fix, retry & learn", type="primary"):
            with st.spinner("Applying fix, retrying step, storing to RAG memory..."):
                updated_pipeline = apply_fix(pipeline, step["id"], diagnosis.fixed_code)
                updated_run = retry_step(run, updated_pipeline, step["id"])
                record_resolved_incident(updated_run, updated_pipeline, step["id"], diagnosis)
            new_status = next(
                sr["status"] for sr in updated_run["step_runs"] if sr["step_id"] == step["id"]
            )
            if new_status == "SUCCEEDED":
                st.success("Fix applied, step succeeded, incident stored to RAG memory ✅")
            else:
                st.warning(f"Fix applied and retried — step now: {new_status}. Incident stored to RAG memory for future reference.")
            del st.session_state["last_diagnosis"]
            st.rerun()
    with c2:
        if st.button("Discard"):
            del st.session_state["last_diagnosis"]
            st.rerun()
