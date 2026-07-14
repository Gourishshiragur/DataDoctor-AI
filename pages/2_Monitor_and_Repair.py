import streamlit as st
import pandas as pd

from core import store
from core.retry_engine import repair_run, retry_step
from core.ui import inject_global_css, sidebar_brand, status_badge

inject_global_css()
sidebar_brand()

st.title("📊 Monitor & Repair")
st.caption("Inspect run history, retry individual failed steps, or repair an entire failed run.")

runs = sorted(store.load_runs(), key=lambda r: r["started_at"], reverse=True)
pipelines_by_id = {p["id"]: p for p in store.load_pipelines()}

if not runs:
    st.info("No runs yet — create and run a pipeline from **Pipeline Builder**.")
    st.stop()

status_filter = st.multiselect(
    "Filter by status",
    ["RUNNING", "SUCCEEDED", "FAILED", "PARTIALLY_REPAIRED"],
    default=["FAILED", "PARTIALLY_REPAIRED", "RUNNING"],
)
filtered = [r for r in runs if r["status"] in status_filter] if status_filter else runs

st.subheader("Run history")
st.dataframe(pd.DataFrame([{
    "Run ID": r["id"], "Pipeline": r["pipeline_name"], "Status": r["status"],
    "Started": r["started_at"][:19].replace("T", " "),
    "Ended": (r.get("ended_at") or "")[:19].replace("T", " "),
    "Steps": len(r.get("step_runs", [])),
    "Rows processed": sum(x.get("rows_processed") or 0 for x in r.get("step_runs", [])),
} for r in filtered]), use_container_width=True, hide_index=True)


for run in filtered:
    pipeline = pipelines_by_id.get(run["pipeline_id"])
    with st.container(border=True):
        header_col, action_col = st.columns([4, 1])
        with header_col:
            st.markdown(
                f"### {run['pipeline_name']} &nbsp; {status_badge(run['status'])}",
                unsafe_allow_html=True,
            )
            st.caption(f"run {run['id']} · started {run['started_at'][:19].replace('T',' ')}")
        with action_col:
            can_repair = run["status"] in ("FAILED", "PARTIALLY_REPAIRED") and pipeline is not None
            if can_repair and st.button("🔧 Repair run", key=f"repair-{run['id']}", type="primary"):
                with st.spinner("Repairing failed steps..."):
                    updated = repair_run(run, pipeline)
                st.success(f"Repair finished — status now {updated['status']}.")
                st.rerun()

        step_rows = []
        for sr in run["step_runs"]:
            meta = next((x for x in (pipeline or {}).get("steps", []) if x["id"] == sr["step_id"]), {})
            step_rows.append({"Step": meta.get("name", sr["step_id"]), "Type": meta.get("step_type", "—"), "Engine": meta.get("engine", "—"), "Status": sr["status"], "Rows": sr.get("rows_processed") or 0, "Retries": sr.get("retry_count", 0), "Error": sr.get("error_message") or ""})
        st.dataframe(pd.DataFrame(step_rows), use_container_width=True, hide_index=True)

        for sr in run["step_runs"]:
            step_meta = None
            if pipeline:
                step_meta = next((s for s in pipeline["steps"] if s["id"] == sr["step_id"]), None)
            step_name = step_meta["name"] if step_meta else sr["step_id"]

            cols = st.columns([3, 2, 2, 1])
            cols[0].markdown(f"**{step_name}**")
            cols[1].markdown(status_badge(sr["status"]), unsafe_allow_html=True)
            if sr.get("error_message"):
                cols[2].markdown(f":red[{sr['error_message']}]")
            elif sr.get("rows_processed"):
                cols[2].markdown(f"{sr['rows_processed']:,} rows")
            retry_disabled = sr["status"] != "FAILED" or sr["retry_count"] >= 3 or pipeline is None
            if cols[3].button(
                f"Retry ({sr['retry_count']}/3)",
                key=f"retry-{run['id']}-{sr['step_id']}",
                disabled=retry_disabled,
            ):
                with st.spinner(f"Retrying '{step_name}'..."):
                    updated = retry_step(run, pipeline, sr["step_id"])
                st.rerun()
