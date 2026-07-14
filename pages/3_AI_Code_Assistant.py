import streamlit as st

from core.ai_assistant import get_code_suggestion
from core.ui import inject_global_css, sidebar_brand

st.set_page_config(page_title="AI Code Assistant · DataDoctor AI", page_icon="🤖", layout="wide")
inject_global_css()
sidebar_brand()

st.title("🤖 AI Code Assistant")
st.caption("Describe what you need in plain language — get a PySpark or SQL snippet back.")

with st.form("suggestion_form"):
    engine = st.radio("Engine", ["pyspark", "sql"], horizontal=True)
    prompt = st.text_area(
        "What do you need?",
        placeholder="e.g. Deduplicate incoming telemetry records keeping the latest by event_time",
        height=90,
    )
    submitted = st.form_submit_button("Generate suggestion", type="primary")

if submitted:
    if not prompt.strip():
        st.error("Describe what you need first.")
    else:
        with st.spinner("Generating..."):
            suggestion = get_code_suggestion(prompt, engine)

        st.markdown(f"**Explanation:** {suggestion.explanation}")
        lang = "python" if engine == "pyspark" else "sql"
        code = suggestion.code.strip("`").replace("python\n", "").replace("sql\n", "")
        st.code(code, language=lang)

        if "claude-api" in suggestion.source:
            st.caption("✅ Generated live via Claude API")
        else:
            st.caption(
                "ℹ️ Generated from local template library (no ANTHROPIC_API_KEY configured — "
                "see README to enable live AI suggestions)."
            )

st.divider()
st.subheader("Common patterns")
st.caption("Click a suggestion to try it instantly.")
examples = [
    "incremental load using a watermark column",
    "SCD2 merge to close and open a version on change",
    "dedup on natural key keeping the latest by event time",
    "Delta MERGE upsert on device_id and event_date",
]
cols = st.columns(2)
for i, ex in enumerate(examples):
    if cols[i % 2].button(ex, key=f"example-{i}"):
        with st.spinner("Generating..."):
            suggestion = get_code_suggestion(ex, "pyspark")
        st.markdown(f"**Explanation:** {suggestion.explanation}")
        code = suggestion.code.strip("`").replace("python\n", "")
        st.code(code, language="python")
