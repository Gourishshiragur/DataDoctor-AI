import streamlit as st

from core import store
from core.ui import inject_global_css, sidebar_brand

inject_global_css()
sidebar_brand()

st.title("📜 Logs")
st.caption("Structured, per-run execution logs — filter by run and severity.")

runs = sorted(store.load_runs(), key=lambda r: r["started_at"], reverse=True)

if not runs:
    st.info("No runs yet.")
    st.stop()

run_options = {f"{r['pipeline_name']} · {r['id']}": r["id"] for r in runs}
c1, c2 = st.columns([3, 1])
selected_label = c1.selectbox("Run", list(run_options.keys()))
level_filter = c2.multiselect("Level", ["INFO", "WARN", "ERROR"], default=["INFO", "WARN", "ERROR"])

run_id = run_options[selected_label]
logs = store.load_logs(run_id)
logs = [l for l in logs if l["level"] in level_filter]

LEVEL_COLOR = {"INFO": "#8B949E", "WARN": "#D29922", "ERROR": "#F85149"}

if not logs:
    st.info("No log entries match the current filter.")
else:
    for entry in logs:
        color = LEVEL_COLOR.get(entry["level"], "#8B949E")
        ts = entry["timestamp"][:19].replace("T", " ")
        st.markdown(
            f"<span style='color:{color}; font-family:monospace'>[{ts}] [{entry['level']}] "
            f"[{entry['step_id']}] {entry['message']}</span>",
            unsafe_allow_html=True,
        )

st.divider()
st.download_button(
    "⬇️ Download logs (JSONL)",
    data="\n".join(str(l) for l in logs),
    file_name=f"{run_id}_logs.txt",
)
