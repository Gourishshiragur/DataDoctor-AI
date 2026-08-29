"""ui/Replay.py — pick a past pipeline run and replay its self-healing narrative
step-by-step, as if watching it happen live again."""
import time

import pandas as pd
import streamlit as st

from database import history
from pipeline.replay import get_run_timeline, list_runs


def render():
    st.title("⏪ Replay")
    st.caption("Step back through any past pipeline run and replay exactly what the self-healing engine did.")

    runs = list_runs(limit=50)
    if not runs:
        st.info("No runs to replay yet. Go run a pipeline in **Pipeline Studio** first.")
        return

    options = {f"{r['run_id']} — {r['dataset']} ({r['status']})": r["run_id"] for r in runs}
    label = st.selectbox("Choose a run", list(options.keys()))
    run_id = options[label]

    run = history.get_run(run_id)
    st.write(f"**Dataset:** {run['dataset']} · **Status:** {run['status']}")

    speed = st.slider("Replay speed (seconds between steps)", 0.0, 1.5, 0.4, 0.1)
    col1, col2 = st.columns([1, 4])
    play = col1.button("▶️ Replay", type="primary")

    timeline = get_run_timeline(run_id)
    if not timeline:
        st.warning("No timeline events recorded for this run.")
        return

    placeholder = st.container()

    if play:
        with placeholder:
            for step in timeline:
                icon = {"event": "ℹ️", "repair": "🩹"}.get(step["type"], "•")
                ts = pd.to_datetime(step["ts"], unit="s").strftime("%H:%M:%S")
                st.write(f"`{ts}` {icon} **[{step['stage']}]** {step['detail']}")
                if speed > 0:
                    time.sleep(speed)
        st.success("Replay complete.")
    else:
        st.caption("Click Replay to step through this run's events, or expand below to see them all at once.")
        with st.expander("Show full timeline instantly"):
            for step in timeline:
                icon = {"event": "ℹ️", "repair": "🩹"}.get(step["type"], "•")
                ts = pd.to_datetime(step["ts"], unit="s").strftime("%H:%M:%S")
                st.write(f"`{ts}` {icon} **[{step['stage']}]** {step['detail']}")
