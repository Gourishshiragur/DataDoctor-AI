import streamlit as st

from core.models import Pipeline, Step, new_id
from core import store
from core.pipeline_engine import run_pipeline
from core.templates import get_templates
from core.ui import inject_global_css, sidebar_brand

st.set_page_config(page_title="Pipeline Builder · DataDoctor AI", page_icon="🛠️", layout="wide")
inject_global_css()
sidebar_brand()

st.title("🛠️ Pipeline Builder")
st.caption("Compose a pipeline as an ordered set of source → transform → sink steps.")

if "draft_steps" not in st.session_state:
    st.session_state.draft_steps = []

with st.expander("⚡ Quick start from a template", expanded=len(st.session_state.draft_steps) == 0):
    templates = get_templates()
    cols = st.columns(len(templates))
    for col, (name, tpl) in zip(cols, templates.items()):
        with col:
            st.markdown(f"**{name}**")
            st.caption(tpl["description"])
            if st.button("Use template", key=f"tpl-{name}"):
                st.session_state.draft_steps = tpl["steps"]
                st.rerun()

with st.expander("➕ Add a step", expanded=True):
    c1, c2, c3 = st.columns([2, 1, 1])
    step_name = c1.text_input("Step name", placeholder="e.g. Load raw telemetry")
    step_type = c2.selectbox("Type", ["source", "transform", "sink"])
    engine = c3.selectbox("Engine", ["pyspark", "sql"])
    code = st.text_area(
        "Code",
        placeholder="df = spark.read.format('delta').load('/mnt/bronze/telemetry')",
        height=100,
    )
    existing_names = [s["name"] for s in st.session_state.draft_steps]
    depends_on = st.multiselect("Depends on", options=existing_names)

    if st.button("Add step", type="primary"):
        if not step_name or not code:
            st.error("Step name and code are required.")
        else:
            dep_ids = [
                s["id"] for s in st.session_state.draft_steps if s["name"] in depends_on
            ]
            st.session_state.draft_steps.append(
                Step(
                    id=new_id("step"),
                    name=step_name,
                    step_type=step_type,
                    engine=engine,
                    code=code,
                    depends_on=dep_ids,
                ).__dict__
            )
            st.rerun()

st.subheader("Draft pipeline steps")
if not st.session_state.draft_steps:
    st.info("No steps added yet.")
else:
    for i, s in enumerate(st.session_state.draft_steps):
        cols = st.columns([5, 1])
        with cols[0]:
            deps = ", ".join(
                d["name"] for d in st.session_state.draft_steps if d["id"] in s["depends_on"]
            ) or "—"
            st.markdown(
                f"**{i+1}. {s['name']}** &nbsp;`{s['step_type']}` · `{s['engine']}` · depends on: {deps}"
            )
            st.code(s["code"], language="python" if s["engine"] == "pyspark" else "sql")
        with cols[1]:
            if st.button("Remove", key=f"remove-{s['id']}"):
                st.session_state.draft_steps.pop(i)
                st.rerun()

st.divider()
st.subheader("Save & run")
p_name = st.text_input("Pipeline name", placeholder="e.g. bronze-silver-gold-telemetry")
p_desc = st.text_input("Description", placeholder="Daily incremental load with SCD2 dimension merge")

c1, c2 = st.columns(2)
with c1:
    if st.button("💾 Save pipeline", disabled=not st.session_state.draft_steps):
        if not p_name:
            st.error("Give the pipeline a name.")
        else:
            pipeline = Pipeline(
                id=new_id("pipe"),
                name=p_name,
                description=p_desc,
                steps=st.session_state.draft_steps,
            )
            store.save_pipeline(pipeline.to_dict())
            st.session_state.draft_steps = []
            st.success(f"Saved pipeline '{p_name}'.")
            st.rerun()

with c2:
    saved = store.load_pipelines()
    options = {p["name"]: p for p in saved}
    if options:
        chosen = st.selectbox("Run a saved pipeline", list(options.keys()))
        if st.button("▶️ Run pipeline", type="primary"):
            with st.spinner(f"Running '{chosen}'..."):
                run = run_pipeline(options[chosen])
            if run["status"] == "SUCCEEDED":
                st.success(f"Run {run['id']} succeeded.")
            else:
                st.error(f"Run {run['id']} finished with status {run['status']}. See Monitor & Repair.")
