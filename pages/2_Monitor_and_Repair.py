import pandas as pd
import streamlit as st

from core import store
from core.retry_engine import (
    MAX_RETRIES_PER_STEP,
    repair_run,
    retry_step,
)
from core.ui import (
    inject_global_css,
    sidebar_brand,
    status_badge,
)


# ---------------------------------------------------------
# PAGE UI
# ---------------------------------------------------------

inject_global_css()
sidebar_brand()

st.title("📊 Monitor & Repair")

st.caption(
    "Inspect pipeline run history, review step-level execution, "
    "retry failed steps, or repair an entire failed run."
)


# ---------------------------------------------------------
# LOAD RUNS AND PIPELINES
# ---------------------------------------------------------

runs = sorted(
    store.load_runs(),
    key=lambda run: run["started_at"],
    reverse=True,
)

pipelines_by_id = {
    pipeline["id"]: pipeline
    for pipeline in store.load_pipelines()
}


# ---------------------------------------------------------
# EMPTY STATE
# ---------------------------------------------------------

if not runs:

    st.info(
        "No pipeline runs are available yet. "
        "Create and run a pipeline from **Pipeline Builder**."
    )

    st.stop()


# ---------------------------------------------------------
# STATUS FILTER
#
# A select box is used instead of a multiselect.
# This prevents the large 'No results' popup from
# overlapping the run-history table.
# ---------------------------------------------------------

status_filter = st.selectbox(
    "Filter by status",
    options=[
        "ALL",
        "FAILED",
        "PARTIALLY_REPAIRED",
        "REPAIRED",
        "RUNNING",
        "SUCCEEDED",
    ],
    index=0,
)


if status_filter == "ALL":

    filtered_runs = runs

else:

    filtered_runs = [
        run
        for run in runs
        if run.get("status") == status_filter
    ]


# ---------------------------------------------------------
# RUN HISTORY
# ---------------------------------------------------------

st.subheader("Run history")


if not filtered_runs:

    st.info(
        f"No pipeline runs currently have the "
        f"status **{status_filter}**."
    )

else:

    run_history_rows = []


    for run in filtered_runs:

        step_runs = run.get(
            "step_runs",
            [],
        )


        run_history_rows.append(
            {
                "Run ID": run.get(
                    "id",
                    "—",
                ),
                "Pipeline": run.get(
                    "pipeline_name",
                    "—",
                ),
                "Status": run.get(
                    "status",
                    "UNKNOWN",
                ),
                "Started": (
                    run.get(
                        "started_at",
                        "",
                    )[:19]
                    .replace(
                        "T",
                        " ",
                    )
                ),
                "Ended": (
                    (
                        run.get(
                            "ended_at"
                        )
                        or ""
                    )[:19]
                    .replace(
                        "T",
                        " ",
                    )
                ),
                "Steps": len(
                    step_runs
                ),
                "Rows processed": sum(
                    step_run.get(
                        "rows_processed"
                    )
                    or 0
                    for step_run
                    in step_runs
                ),
                "Retries": sum(
                    step_run.get(
                        "retry_count",
                        0,
                    )
                    for step_run
                    in step_runs
                ),
            }
        )


    st.dataframe(
        pd.DataFrame(
            run_history_rows
        ),
        use_container_width=True,
        hide_index=True,
    )


# ---------------------------------------------------------
# RUN DETAILS
# ---------------------------------------------------------

for run in filtered_runs:


    pipeline = pipelines_by_id.get(
        run.get(
            "pipeline_id"
        )
    )


    with st.container(
        border=True
    ):


        # -------------------------------------------------
        # RUN HEADER
        # -------------------------------------------------

        header_column, action_column = (

            st.columns(
                [
                    4,
                    1,
                ]
            )

        )


        with header_column:


            st.markdown(
                (
                    f"### "
                    f"{run.get('pipeline_name', 'Unknown pipeline')} "
                    f"&nbsp; "
                    f"{status_badge(run.get('status', 'UNKNOWN'))}"
                ),
                unsafe_allow_html=True,
            )


            started_at = (

                run.get(
                    "started_at",
                    "",
                )[:19]
                .replace(
                    "T",
                    " ",
                )

            )


            run_caption = (

                f"Run {run.get('id', '—')} "
                f"· started {started_at}"

            )


            if (
                run.get(
                    "status"
                )
                == "REPAIRED"
            ):


                total_retries = sum(

                    step_run.get(
                        "retry_count",
                        0,
                    )

                    for step_run

                    in run.get(
                        "step_runs",
                        [],
                    )

                )


                run_caption += (

                    f" · recovered after "
                    f"{total_retries} "
                    f"retry attempt(s)"

                )


            st.caption(
                run_caption
            )


        # -------------------------------------------------
        # REPAIR BUTTON
        # -------------------------------------------------

        with action_column:


            can_repair = (

                run.get(
                    "status"
                )

                in (
                    "FAILED",
                    "PARTIALLY_REPAIRED",
                )

                and pipeline
                is not None

            )


            if (

                can_repair

                and st.button(

                    "🔧 Repair run",

                    key=(
                        f"repair-"
                        f"{run.get('id')}"
                    ),

                    type="primary",

                    use_container_width=True,

                )

            ):


                with st.spinner(

                    "Repairing failed and "
                    "skipped pipeline steps..."

                ):


                    updated_run = (

                        repair_run(

                            run,

                            pipeline,

                        )

                    )


                if (

                    updated_run.get(
                        "status"
                    )

                    == "REPAIRED"

                ):


                    st.success(

                        "Repair completed successfully. "
                        "The pipeline recovered and is "
                        "now marked as REPAIRED."

                    )


                elif (

                    updated_run.get(
                        "status"
                    )

                    == "PARTIALLY_REPAIRED"

                ):


                    st.warning(

                        "The pipeline was partially repaired, "
                        "but one or more failed or skipped "
                        "steps remain."

                    )


                else:


                    st.error(

                        "The repair completed, but unresolved "
                        "pipeline failures remain."

                    )


                st.rerun()


        # -------------------------------------------------
        # STEP EXECUTION TABLE
        # -------------------------------------------------

        st.markdown(
            "#### Step execution"
        )


        step_rows = []


        for step_run in run.get(
            "step_runs",
            [],
        ):


            step_metadata = next(

                (

                    step

                    for step

                    in (
                        pipeline
                        or {}
                    ).get(
                        "steps",
                        [],
                    )

                    if step.get(
                        "id"
                    )

                    == step_run.get(
                        "step_id"
                    )

                ),

                {},

            )


            step_rows.append(
                {
                    "Step": (
                        step_metadata.get(
                            "name"
                        )
                        or step_run.get(
                            "step_id",
                            "—",
                        )
                    ),
                    "Type": step_metadata.get(
                        "step_type",
                        "—",
                    ),
                    "Engine": step_metadata.get(
                        "engine",
                        "—",
                    ),
                    "Status": step_run.get(
                        "status",
                        "UNKNOWN",
                    ),
                    "Rows": (
                        step_run.get(
                            "rows_processed"
                        )
                        or 0
                    ),
                    "Retries": step_run.get(
                        "retry_count",
                        0,
                    ),
                    "Error": (
                        step_run.get(
                            "error_message"
                        )
                        or ""
                    ),
                }
            )


        st.dataframe(
            pd.DataFrame(
                step_rows
            ),
            use_container_width=True,
            hide_index=True,
        )


        # -------------------------------------------------
        # INDIVIDUAL STEP ACTIONS
        # -------------------------------------------------

        st.markdown(
            "#### Step actions"
        )


        for step_run in run.get(
            "step_runs",
            [],
        ):


            step_metadata = None


            if pipeline:


                step_metadata = next(

                    (

                        step

                        for step

                        in pipeline.get(
                            "steps",
                            [],
                        )

                        if step.get(
                            "id"
                        )

                        == step_run.get(
                            "step_id"
                        )

                    ),

                    None,

                )


            step_name = (

                step_metadata.get(
                    "name"
                )

                if step_metadata

                else step_run.get(
                    "step_id",
                    "Unknown step",
                )

            )


            (
                name_column,
                status_column,
                detail_column,
                retry_column,
            ) = st.columns(
                [
                    3,
                    2,
                    3,
                    1.5,
                ]
            )


            name_column.markdown(

                f"**{step_name}**"

            )


            status_column.markdown(

                status_badge(

                    step_run.get(
                        "status",
                        "UNKNOWN",
                    )

                ),

                unsafe_allow_html=True,

            )


            if step_run.get(

                "error_message"

            ):


                detail_column.markdown(

                    f":red["
                    f"{step_run['error_message']}"
                    f"]"

                )


            elif step_run.get(

                "rows_processed"

            ):


                detail_column.markdown(

                    (
                        f"{step_run['rows_processed']:,} "
                        f"rows processed"
                    )

                )


            else:


                detail_column.markdown(

                    "No execution details"

                )


            retry_count = (

                step_run.get(
                    "retry_count",
                    0,
                )

            )


            retry_disabled = (

                step_run.get(
                    "status"
                )
                != "FAILED"

               or retry_count >= MAX_RETRIES_PER_STEP

                or pipeline is None

            )


            if retry_column.button(

                (
                    f"Retry "
                    f"({retry_count}/{MAX_RETRIES_PER_STEP})"
                ),

                key=(

                    f"retry-"

                    f"{run.get('id')}-"

                    f"{step_run.get('step_id')}"

                ),

                disabled=(
                    retry_disabled
                ),

                use_container_width=True,

            ):


                with st.spinner(

                    f"Retrying "
                    f"'{step_name}'..."

                ):


                    updated_run = (

                        retry_step(

                            run,

                            pipeline,

                            step_run[
                                "step_id"
                            ],

                        )

                    )


                if (

                    updated_run.get(
                        "status"
                    )

                    == "REPAIRED"

                ):


                    st.success(

                        "The failed step recovered "
                        "successfully. The run is now "
                        "marked as REPAIRED."

                    )


                st.rerun()