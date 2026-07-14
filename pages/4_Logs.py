"""
DataDoctor AI — Structured Execution Logs

Displays pipeline execution logs with run-level and severity
filters and supports export using valid JSONL format.
"""

import json

import streamlit as st

from core import store
from core.ui import (
    inject_global_css,
    sidebar_brand,
)


# ============================================================
# Page UI
# ============================================================

inject_global_css()

sidebar_brand()


st.title(
    "📜 Logs"
)


st.caption(
    "Structured, per-run execution logs — "
    "filter by pipeline run and severity."
)


# ============================================================
# Load pipeline runs
# ============================================================

runs = sorted(

    store.load_runs(),

    key=lambda run: (
        run.get(
            "started_at",
            ""
        )
    ),

    reverse=True,

)


# ============================================================
# Empty state
# ============================================================

if not runs:

    st.info(
        "No pipeline runs are available yet. "
        "Create and execute a pipeline from "
        "**Pipeline Builder**."
    )

    st.stop()


# ============================================================
# Run-selection options
# ============================================================

run_options = {


    (
        f"{run.get('pipeline_name', 'Unknown pipeline')} "
        f"· {run.get('id', 'Unknown run')}"
    ):

    run.get(
        "id"
    )


    for run in runs

    if run.get(
        "id"
    )

}


# ============================================================
# Log filters
# ============================================================

run_column, level_column = (

    st.columns(
        [
            3,
            1,
        ]
    )

)


selected_label = (

    run_column.selectbox(

        "Run",

        options=list(
            run_options.keys()
        ),

    )

)


level_filter = (

    level_column.multiselect(

        "Level",

        options=[
            "INFO",
            "WARN",
            "ERROR",
        ],

        default=[
            "INFO",
            "WARN",
            "ERROR",
        ],

    )

)


run_id = (

    run_options[
        selected_label
    ]

)


# ============================================================
# Load and filter logs
# ============================================================

logs = (

    store.load_logs(
        run_id
    )

)


logs = [

    log

    for log in logs

    if log.get(
        "level",
        "INFO",
    )
    in level_filter

]


# ============================================================
# Log-level display colors
# ============================================================

LEVEL_COLOR = {

    "INFO": (
        "#8B949E"
    ),

    "WARN": (
        "#D29922"
    ),

    "ERROR": (
        "#F85149"
    ),

}


# ============================================================
# Log display
# ============================================================

if not logs:


    st.info(
        "No log entries match the current filters."
    )


else:


    for entry in logs:


        level = entry.get(
            "level",
            "INFO",
        )


        color = LEVEL_COLOR.get(

            level,

            "#8B949E",

        )


        timestamp = (

            entry.get(
                "timestamp",
                "",
            )[:19]

            .replace(
                "T",
                " ",
            )

        )


        step_id = entry.get(

            "step_id",

            "—",

        )


        message = entry.get(

            "message",

            "",

        )


        st.markdown(

            (
                "<span "
                f"style='color:{color}; "
                "font-family:monospace'>"
                f"[{timestamp}] "
                f"[{level}] "
                f"[{step_id}] "
                f"{message}"
                "</span>"
            ),

            unsafe_allow_html=True,

        )


# ============================================================
# JSONL export
# ============================================================

st.divider()


jsonl_content = (

    "\n".join(

        json.dumps(

            log,

            ensure_ascii=False,

            default=str,

        )

        for log in logs

    )

)


if jsonl_content:

    jsonl_content += "\n"


st.download_button(

    "⬇️ Download logs (JSONL)",

    data=jsonl_content,

    file_name=(

        f"{run_id}_logs.jsonl"

    ),

    mime=(

        "application/x-ndjson"

    ),

    use_container_width=False,

)