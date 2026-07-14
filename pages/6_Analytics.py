from collections import Counter
from datetime import datetime

import streamlit as st

from core import store
from core.ui import inject_global_css, sidebar_brand

inject_global_css()
sidebar_brand()

st.title("📈 Analytics")
st.caption("Run trends, reliability, and the most common failure patterns across all pipelines.")

runs = store.load_runs()

if not runs:
    st.info("No runs yet — create and run a pipeline from **Pipeline Builder** to see analytics here.")
    st.stop()

# ---- summary metrics ----
total = len(runs)
succeeded = sum(1 for r in runs if r["status"] == "SUCCEEDED")
failed = sum(1 for r in runs if r["status"] == "FAILED")
repaired = sum(1 for r in runs if r["status"] == "PARTIALLY_REPAIRED")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total runs", total)
c2.metric("Success rate", f"{succeeded / total * 100:.0f}%")
c3.metric("Failed (unresolved)", failed)
c4.metric("Repaired", repaired)

st.divider()

left, right = st.columns(2)

# ---- runs per day ----
with left:
    st.subheader("Runs over time")
    by_day = Counter()
    for r in runs:
        day = r["started_at"][:10]
        by_day[day] += 1
    days_sorted = sorted(by_day.keys())
    if days_sorted:
        st.bar_chart({"runs": [by_day[d] for d in days_sorted]})
        st.caption(f"{days_sorted[0]} → {days_sorted[-1]}")

# ---- status breakdown ----
with right:
    st.subheader("Status breakdown")
    status_counts = Counter(r["status"] for r in runs)
    st.bar_chart(status_counts)

st.divider()

# ---- most common failure reasons ----
st.subheader("Most common failure reasons")
error_counter = Counter()
for r in runs:
    for sr in r["step_runs"]:
        if sr.get("error_message"):
            # bucket by first ~40 chars / exception type to group similar errors
            key = sr["error_message"].split(":")[0]
            error_counter[key] += 1

if not error_counter:
    st.success("No step failures recorded yet.")
else:
    for reason, count in error_counter.most_common(8):
        cols = st.columns([4, 1])
        cols[0].write(reason)
        cols[1].write(f"**{count}×**")

st.divider()

# ---- pipeline-level reliability ----
st.subheader("Reliability by pipeline")
pipelines_by_name = Counter(r["pipeline_name"] for r in runs)
for name in pipelines_by_name:
    pipeline_runs = [r for r in runs if r["pipeline_name"] == name]
    pipeline_success = sum(1 for r in pipeline_runs if r["status"] == "SUCCEEDED")
    rate = pipeline_success / len(pipeline_runs) * 100
    st.markdown(f"**{name}** — {len(pipeline_runs)} runs, {rate:.0f}% success rate")
    st.progress(rate / 100)
