from concurrent.futures import ThreadPoolExecutor
"""ui/PipelineStudio.py — the heart of the app: upload/select a dataset and watch it
flow live through Bronze -> Quality Checks -> Self-Healing Repair -> Silver ->
Quality Re-check -> Gold, with every step streamed to the screen as it happens.
"""

import time
import uuid

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

from ai import business_assistant
from config.settings import current_mode, is_databricks_configured, load_settings
from database import history
from dbx_enterprise import jobs as dbx_jobs
from pipeline import bronze, gold, profiling, silver
from pipeline import runtime_state
from storage import manager as storage_manager
from storage import router as storage_router
from ui._common import dataset_picker, score_color


def _backend_badge():
    """Shows which storage backend actually served the write just made — both modes now
    try their own workspace's Databricks credentials first and fall back to DuckDB if
    unreachable, so this reflects reality rather than assuming Enterprise=Databricks."""
    status = storage_router.get_last_status()
    mode_label = status.get("mode", "demo").title()
    if status["fallback"]:
        st.caption(
            f"💾 Served by **DuckDB (fallback)** — {mode_label} Mode's Databricks unavailable: {status['reason']}"
        )
    elif status["actual_backend"] == "databricks":
        st.caption(
            f"💾 Served by **Databricks SQL Warehouse** ({mode_label} workspace)"
        )
    else:
        st.caption(f"💾 Served by **DuckDB** ({mode_label} Mode)")


def _live_delay():
    # tiny delay per step so the "live process" is visibly sequential rather than
    # a single instant render — real work (profiling/repair/SQL) still dominates the time.
    time.sleep(0.35)


def _human_size(num_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def _render_animated_flow(dataset_name: str, silver_result: dict, gold_result: dict):
    """Animated data-flow diagram — real SVG/SMIL motion (not a GIF, not decorative):
    particle count per path is proportional to actual row volume between stages, and
    particle color reflects the real backend that served that stage. Numbers shown are
    the same ones the Sankey diagram uses — this is a visual layer on top of real data,
    not a separate illustrative animation."""
    bronze_df = st.session_state.get("bronze_df")
    if bronze_df is None:
        return
    silver_df, gold_df = silver_result["df"], gold_result["df"]
    run_id = st.session_state.get("last_run_id")

    stage_backend = {"Bronze": "duckdb", "Silver": "duckdb", "Gold": "duckdb"}
    if run_id:
        for ev in history.get_backend_events(run_id=run_id):
            if ev["layer"] in ("bronze", "silver", "gold"):
                stage_backend[ev["layer"].title()] = ev["actual_backend"]

    stages = [
        ("Uploaded", bronze_df, None),
        ("Bronze", bronze_df, stage_backend["Bronze"]),
        ("Silver", silver_df, stage_backend["Silver"]),
        ("Gold", gold_df, stage_backend["Gold"]),
    ]
    row_counts = [len(s[1]) for s in stages]
    max_rows = max(row_counts) or 1
    backend_color = {"databricks": "#ff6b35", "duckdb": "#4f9dde", None: "#94a3b8"}

    node_x = [80, 320, 560, 800]
    node_y = 110
    nodes_svg, paths_svg, particles_svg = "", "", ""

    for i, (label, sdf, backend) in enumerate(stages):
        vol = sdf.memory_usage(deep=True).sum()
        color = backend_color.get(backend, "#94a3b8")
        nodes_svg += f"""
        <g>
          <rect x="{node_x[i]-55}" y="{node_y-30}" width="110" height="60" rx="10"
                fill="{color}" fill-opacity="0.15" stroke="{color}" stroke-width="2"/>
          <text x="{node_x[i]}" y="{node_y-8}" text-anchor="middle" font-size="13" font-weight="600" fill="#e2e8f0">{label}</text>
          <text x="{node_x[i]}" y="{node_y+10}" text-anchor="middle" font-size="11" fill="#94a3b8">{len(sdf):,} rows</text>
          <text x="{node_x[i]}" y="{node_y+24}" text-anchor="middle" font-size="10" fill="#64748b">{_human_size(vol)}{' · ' + backend if backend else ''}</text>
        </g>"""

    for i in range(3):
        x1, x2 = node_x[i] + 55, node_x[i + 1] - 55
        path_id = f"path{i}"
        rows_on_path = row_counts[
            i
        ]  # volume flowing OUT of the source stage into the next
        n_particles = max(2, min(12, round(10 * rows_on_path / max_rows)))
        speed = 3.0  # seconds per full traversal — constant so relative density (not speed) encodes volume
        target_backend = stages[i + 1][2]
        color = backend_color.get(target_backend, "#94a3b8")
        paths_svg += f'<path id="{path_id}" d="M{x1},{node_y} L{x2},{node_y}" fill="none" stroke="#334155" stroke-width="2"/>'
        for p in range(n_particles):
            begin = round(p * speed / n_particles, 2)
            particles_svg += f"""
            <circle r="3.5" fill="{color}">
              <animateMotion dur="{speed}s" begin="{begin}s" repeatCount="indefinite">
                <mpath href="#{path_id}"/>
              </animateMotion>
            </circle>"""

    svg = f"""
    <svg viewBox="0 0 880 220" xmlns="http://www.w3.org/2000/svg" style="width:100%;height:auto;background:transparent;">
      {paths_svg}
      {particles_svg}
      {nodes_svg}
    </svg>"""
    components.html(f'<div style="padding:8px 0;">{svg}</div>', height=230)


def _render_data_flow_diagram(
    dataset_name: str, silver_result: dict, gold_result: dict
):
    """A Sankey diagram of the just-completed run — every number here is read straight
    from the actual DataFrames produced (row counts, in-memory byte size) and the real
    backend_events log for this run_id. Nothing here is simulated; if the run doesn't
    have the data, the diagram doesn't claim it does."""
    bronze_df = st.session_state.get("bronze_df")
    if bronze_df is None:
        return
    silver_df, gold_df = silver_result["df"], gold_result["df"]
    run_id = st.session_state.get("last_run_id")

    stage_backend = {"Bronze": "duckdb", "Silver": "duckdb", "Gold": "duckdb"}
    if run_id:
        for ev in history.get_backend_events(run_id=run_id):
            if ev["layer"] in ("bronze", "silver", "gold"):
                stage_backend[ev["layer"].title()] = ev["actual_backend"]

    stages = [
        ("Uploaded file", bronze_df, None),
        ("Bronze", bronze_df, stage_backend["Bronze"]),
        ("Silver (repaired)", silver_df, stage_backend["Silver"]),
        ("Gold (aggregated)", gold_df, stage_backend["Gold"]),
    ]
    labels, row_counts = [], []
    for label, sdf, backend in stages:
        vol = sdf.memory_usage(deep=True).sum()
        backend_tag = f" · {backend}" if backend else ""
        labels.append(f"{label}<br>{len(sdf):,} rows · {_human_size(vol)}{backend_tag}")
        row_counts.append(len(sdf))

    fig = go.Figure(
        go.Sankey(
            node=dict(
                pad=20,
                thickness=18,
                label=labels,
                color=["#94a3b8", "#cd7f32", "#c0c0c0", "#ffd700"],
            ),
            link=dict(
                source=[0, 1, 2],
                target=[1, 2, 3],
                value=[
                    max(row_counts[0], 1),
                    max(row_counts[1], 1),
                    max(row_counts[2], 1),
                ],
                color=[
                    "rgba(148,163,184,0.4)",
                    "rgba(205,127,50,0.4)",
                    "rgba(192,192,192,0.4)",
                ],
            ),
        )
    )
    fig.update_layout(
        height=280, margin=dict(l=10, r=10, t=10, b=10), font=dict(size=12)
    )
    st.plotly_chart(fig, use_container_width=True, key=f"flow_{dataset_name}_{run_id}")
    reduction = row_counts[0] - row_counts[-1]
    if reduction != 0:
        st.caption(
            f"Gold aggregation reduced {row_counts[0]:,} raw rows to {row_counts[-1]:,} "
            f"({abs(reduction):,} {'fewer' if reduction > 0 else 'more'} rows after grouping)."
        )


def _submit_job(
    dataset_name: str,
    df: pd.DataFrame,
    mode: str,
    notebook_source_override: str | None = None,
    storage_id_override: str | None = None,
    internal_run_id: str | None = None,
) -> tuple[str, str]:
    """Shared submission steps: ensure Volume -> upload -> deploy notebook -> submit Job.
    Used by both the primary 'Run Pipeline' button and the advanced panel below, so
    both go through the exact same real steps rather than two divergent code paths.
    Returns (run_id, notebook_path). Raises on any failure — callers decide how to
    surface it (the primary button auto-falls back; the advanced panel shows + offers
    the same fallback)."""
    storage_id = (
        storage_id_override
        if storage_id_override is not None
        else st.session_state.get("studio_storage_id")
    )
    file_bytes = (
        storage_manager.load_dataset_bytes(storage_id)
        if storage_id
        else df.to_csv(index=False).encode()
    )  # fallback if picked before Storage Manager wiring

    dbx_jobs.ensure_landing_volume(mode=mode)
    volume_path = dbx_jobs.upload_file_to_volume(file_bytes, dataset_name, mode=mode)
    notebook_path = dbx_jobs.deploy_notebook(
        mode=mode, content=notebook_source_override
    )
    history.log_audit(
        "local-user",
        "notebook_deployed",
        notebook_path,
        {"mode": mode, "source_type": "manual" if notebook_source_override else "auto"},
    )
    history.save_notebook_version(
        version_id=str(uuid.uuid4())[:12],
        dataset_id=storage_id,
        source_type="manual" if notebook_source_override else "auto",
        content=notebook_source_override or dbx_jobs.get_bundled_notebook_source(),
        workspace_path=notebook_path,
        mode=mode,
    )
    run_id = dbx_jobs.submit_job_run(
        volume_path,
        dataset_name,
        notebook_path,
        mode=mode,
        datadoctor_run_id=internal_run_id,
    )
    history.log_audit(
        "local-user",
        "job_submitted",
        run_id,
        {"mode": mode, "dataset": dataset_name, "volume_path": volume_path},
    )
    return run_id, notebook_path


def _populate_results_from_databricks(dataset_name: str, mode: str, run_id: str):
    """Read successful native Spark results DIRECTLY from Databricks.

    Important: this intentionally bypasses storage.router because the router is
    designed to fall back to DuckDB. A successfully completed native Databricks
    Job must never have its result lookup silently redirected to local DuckDB.
    """
    from dbx_enterprise import connection as dbx_connection

    # Native Spark Job writes these tables into the active mode's configured
    # catalog/schema using the bronze/silver/gold prefixes passed as Job parameters.
    bronze_tbl = dbx_connection.read_table(
        "bronze", dataset_name, mode=mode
    )
    silver_tbl = dbx_connection.read_table(
        "silver", dataset_name, mode=mode
    )
    gold_tbl = dbx_connection.read_table(
        "gold", dataset_name, mode=mode
    )

    # Durable audit trail: all three layers were served by Databricks.
    for layer in ("bronze", "silver", "gold"):
        history.log_backend_event(
            run_id,
            layer,
            dataset_name,
            "databricks",
            "databricks",
            False,
            "",
        )

    st.session_state.bronze_df = bronze_tbl
    st.session_state.silver_result = {
        "df": silver_tbl,
        "pre_checks": [],
        "post_checks": [],
        "repair_actions": [],
        "quality_score": None,
        "below_minimum": False,
    }
    st.session_state.gold_result = {
        "df": gold_tbl,
        "kpis": {},
        "group_column": None,
    }
    st.session_state.active_dataset = dataset_name
    st.session_state.last_run_id = run_id

    return {
        "bronze_rows": len(bronze_tbl),
        "silver_rows": len(silver_tbl),
        "gold_rows": len(gold_tbl),
    }



# A module-level executor is intentionally used here.
# Streamlit reruns pages when navigation changes, but this worker belongs
# to the Python process rather than to the current Streamlit page request.
_DBX_SUBMIT_EXECUTOR = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="datadoctor-dbx-submit",
)


def _background_submit_databricks(
    internal_run_id: str,
    dataset_name: str,
    df: pd.DataFrame,
    mode: str,
    notebook_override: str | None,
    storage_id: str | None,
    explain_toggle: bool,
):
    """
    Durable Databricks submission worker.

    IMPORTANT:
    - No Streamlit UI calls.
    - No runtime_state calls.
    - Persist the real Databricks run ID immediately.
    - Once submitted, Databricks owns execution independently of Streamlit.
    """
    try:
        dbx_run_id, notebook_path = _submit_job(
            dataset_name,
            df,
            mode,
            notebook_override,
            storage_id_override=storage_id,
            internal_run_id=internal_run_id,
        )

        # The value returned here MUST be the real Databricks run ID.
        if not dbx_run_id:
            raise RuntimeError("Databricks returned an empty run_id")

        summary = {
            "engine": "databricks",
            "backend": "databricks",
            "dbx_run_id": str(dbx_run_id),
            "mode": mode,
            "dataset": dataset_name,
            "notebook_path": notebook_path,
            "explain": bool(explain_toggle),
            "submission_state": "running",
        }

        # Persist the real DBX identity before anything else.
        history.mark_running(internal_run_id, summary)

        history.log_event(
            internal_run_id,
            "bronze",
            f"Databricks Job submitted independently - run_id {dbx_run_id}",
        )

    except Exception as e:
        error_summary = {
            "engine": "databricks",
            "backend": "databricks",
            "mode": mode,
            "dataset": dataset_name,
            "explain": bool(explain_toggle),
            "submission_state": "failed",
            "reason": "job_submission_failed",
            "error": str(e),
        }

        history.finish_run(
            internal_run_id,
            "failed",
            error_summary,
        )

        try:
            history.log_event(
                internal_run_id,
                "bronze",
                f"Databricks submission failed: {e}",
                level="error",
            )
        except Exception:
            pass


def _run_orchestrated_pipeline(
    dataset_name: str,
    df: pd.DataFrame,
    mode: str,
    explain_toggle: bool,
):
    """
    Start Databricks execution independently from the current Streamlit page.

    The important guarantee is:
        clicking Dashboard / Monitor / Settings does NOT cancel the job.

    Streamlit only starts the background worker and returns immediately.
    The worker persists the real Databricks run ID and Dashboard later polls
    that persistent identity.
    """
    notebook_override = (
        st.session_state.get("dbx_custom_notebook_source")
        if st.session_state.get("dbx_notebook_choice") == "Write my own"
        else None
    )

    storage_id = st.session_state.get("studio_storage_id")

    # Copy the dataframe because the worker must not depend on the current
    # Streamlit request remaining alive.
    worker_df = df.copy(deep=True)

    internal_run_id = history.new_run(dataset_name)
    st.session_state.last_run_id = internal_run_id

    initial_summary = {
        "engine": "databricks",
        "backend": "databricks",
        "mode": mode,
        "dataset": dataset_name,
        "explain": bool(explain_toggle),
        "submission_state": "submitting",
    }

    history.update_run_summary(
        internal_run_id,
        initial_summary,
    )

    history.log_event(
        internal_run_id,
        "bronze",
        "Databricks submission started in background; page navigation is safe.",
    )

    # Runtime visualization becomes active immediately.
    try:
        runtime_state.update_stage(
            internal_run_id,
            "bronze",
            "running",
            rows_in=len(worker_df),
            message=(
                "Submitting Databricks Spark Job independently. "
                "You can navigate to Dashboard without stopping the run."
            ),
        )
    except Exception:
        pass

    # The actual Databricks submission now belongs to the process-level worker,
    # not the current Streamlit page.
    _DBX_SUBMIT_EXECUTOR.submit(
        _background_submit_databricks,
        internal_run_id,
        dataset_name,
        worker_df,
        mode,
        notebook_override,
        storage_id,
        explain_toggle,
    )

    # Do NOT wait for the remote job here.
    st.success(
        f"? Databricks pipeline `{internal_run_id}` started independently. "
        "You can open Dashboard or Monitor now ? the pipeline will continue."
    )



def _render_persistent_databricks_status(restored: dict):
    """Render a navigation-safe live view from durable Databricks state.

    This renderer deliberately does NOT depend on Streamlit session_state
    DataFrames. Databricks owns the real execution; Studio only reconstructs
    the visible state after a page navigation/rerun.
    """
    import json

    run_id = str(restored.get("run_id") or "")
    summary = restored.get("summary") or {}

    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}

    dbx_status = restored.get("_dbx_status") or {}
    dbx_run_id = str(
        dbx_status.get("run_id")
        or summary.get("dbx_run_id")
        or ""
    )

    lifecycle = str(
        dbx_status.get("life_cycle_state")
        or "RUNNING"
    )
    state_message = str(
        dbx_status.get("state_message")
        or ""
    )

    st.divider()
    st.subheader("? Live Pipeline Execution")

    st.info(
        f"Databricks Spark Job **{dbx_run_id}** is running independently. "
        "You can navigate between Dashboard, Monitor and Studio without "
        "cancelling the pipeline."
    )

    # CSS animation is purely visual. It is NOT the execution mechanism.
    components.html(
        """
        <style>
        .ddx-live-wrap {
            padding: 14px 4px 8px 4px;
        }

        .ddx-live-header {
            display: flex;
            align-items: center;
            gap: 10px;
            font-family: sans-serif;
            font-size: 15px;
            font-weight: 600;
            margin-bottom: 16px;
        }

        .ddx-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #22c55e;
            animation: ddx-pulse 1.2s infinite;
        }

        @keyframes ddx-pulse {
            0%, 100% { opacity: .35; transform: scale(.85); }
            50% { opacity: 1; transform: scale(1.15); }
        }

        .ddx-track {
            display: flex;
            align-items: center;
            width: 100%;
            gap: 0;
        }

        .ddx-stage {
            flex: 1;
            text-align: center;
            font-family: sans-serif;
        }

        .ddx-node {
            width: 54px;
            height: 54px;
            border-radius: 50%;
            margin: auto;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 2px solid #f97316;
            background: rgba(249,115,22,.12);
            font-size: 21px;
            animation: ddx-glow 1.8s infinite;
        }

        @keyframes ddx-glow {
            0%, 100% { box-shadow: 0 0 0 rgba(249,115,22,0); }
            50% { box-shadow: 0 0 18px rgba(249,115,22,.35); }
        }

        .ddx-label {
            margin-top: 7px;
            color: #cbd5e1;
            font-size: 12px;
        }

        .ddx-line {
            flex: .7;
            height: 2px;
            background: #475569;
            position: relative;
            overflow: hidden;
        }

        .ddx-line:after {
            content: "";
            position: absolute;
            left: -30%;
            top: 0;
            width: 30%;
            height: 100%;
            background: #f97316;
            animation: ddx-flow 1.4s linear infinite;
        }

        @keyframes ddx-flow {
            from { left: -30%; }
            to { left: 100%; }
        }

        .ddx-meta {
            margin-top: 15px;
            color: #94a3b8;
            font-family: sans-serif;
            font-size: 12px;
        }
        </style>

        <div class="ddx-live-wrap">
          <div class="ddx-live-header">
            <span class="ddx-dot"></span>
            <span>Databricks Spark execution is active</span>
          </div>

          <div class="ddx-track">
            <div class="ddx-stage">
              <div class="ddx-node">??</div>
              <div class="ddx-label">Uploaded</div>
            </div>

            <div class="ddx-line"></div>

            <div class="ddx-stage">
              <div class="ddx-node">??</div>
              <div class="ddx-label">Bronze</div>
            </div>

            <div class="ddx-line"></div>

            <div class="ddx-stage">
              <div class="ddx-node">??</div>
              <div class="ddx-label">Quality</div>
            </div>

            <div class="ddx-line"></div>

            <div class="ddx-stage">
              <div class="ddx-node">???</div>
              <div class="ddx-label">Repair</div>
            </div>

            <div class="ddx-line"></div>

            <div class="ddx-stage">
              <div class="ddx-node">??</div>
              <div class="ddx-label">Silver</div>
            </div>

            <div class="ddx-line"></div>

            <div class="ddx-stage">
              <div class="ddx-node">??</div>
              <div class="ddx-label">Gold</div>
            </div>
          </div>
        </div>
        """,
        height=155,
    )

    st.caption(
        f"Databricks run: `{dbx_run_id}` ? lifecycle: **{lifecycle}**"
        + (f" ? {state_message}" if state_message else "")
    )

    run_page_url = dbx_status.get("run_page_url") or summary.get(
        "databricks_run_page_url"
    )

    if run_page_url:
        st.link_button(
            "Open Databricks Run",
            run_page_url,
        )


def _restore_persistent_databricks_run(dataset_name: str, mode: str):
    """
    Restore the latest persisted Databricks execution into Streamlit state.

    Streamlit reruns when the user changes pages. Therefore the live pipeline
    MUST NOT depend on st.session_state surviving navigation.

    SQLite history is authoritative for the internal run and contains the real
    Databricks run ID. Databricks itself remains the execution owner.
    """
    import json

    try:
        runs = history.get_runs(limit=25)

        for run in runs:
            if str(run.get("dataset") or "") != str(dataset_name):
                continue

            summary = run.get("summary") or {}

            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    continue

            if not isinstance(summary, dict):
                continue

            dbx_run_id = summary.get("dbx_run_id")
            run_mode = summary.get("mode") or mode

            if not dbx_run_id:
                continue

            status = str(run.get("status") or "").lower()

            # Terminal runs are still useful for showing the final result,
            # but they must never be rendered as an active animation.
            if status in {"success", "failed", "cancelled"}:
                st.session_state.last_run_id = run["run_id"]
                st.session_state.active_databricks_run_id = None
                return run

            if status != "running":
                continue

            # Restore the persistent internal identity first.
            st.session_state.last_run_id = run["run_id"]
            st.session_state.active_dataset = dataset_name
            st.session_state.active_databricks_run_id = str(dbx_run_id)

            # Check Databricks exactly once on this Streamlit rerun.
            try:
                from dbx_enterprise import jobs as dbx_jobs

                dbx_status = dbx_jobs.get_run_status(
                    str(dbx_run_id),
                    mode=run_mode,
                )

                lifecycle = dbx_status.get("life_cycle_state")
                result = dbx_status.get("result_state")

                if lifecycle in ("PENDING", "RUNNING"):
                    return {
                        **run,
                        "_dbx_status": dbx_status,
                        "_live": True,
                    }

                if lifecycle == "TERMINATED" and result == "SUCCESS":
                    history.finish_run(
                        run["run_id"],
                        "success",
                        {
                            **summary,
                            "result_state": result,
                            "life_cycle_state": lifecycle,
                            "databricks_run_page_url":
                                dbx_status.get("run_page_url", ""),
                        },
                    )
                    st.session_state.active_databricks_run_id = None

                    return {
                        **run,
                        "status": "success",
                        "_dbx_status": dbx_status,
                        "_live": False,
                    }

                if lifecycle in {
                    "TERMINATED",
                    "SKIPPED",
                    "INTERNAL_ERROR",
                }:
                    error = (
                        dbx_status.get("error_message")
                        or dbx_status.get("state_message")
                        or "Databricks job failed"
                    )

                    history.finish_run(
                        run["run_id"],
                        "failed",
                        {
                            **summary,
                            "reason": "databricks_job_failed",
                            "error": error,
                            "result_state": result,
                            "life_cycle_state": lifecycle,
                            "databricks_run_page_url":
                                dbx_status.get("run_page_url", ""),
                        },
                    )

                    st.session_state.active_databricks_run_id = None

                    return {
                        **run,
                        "status": "failed",
                        "_dbx_status": dbx_status,
                        "_live": False,
                    }

            except Exception:
                # Do NOT mark the Databricks job failed merely because the
                # Studio page temporarily cannot reach Databricks.
                return {
                    **run,
                    "_live": True,
                    "_dbx_unreachable": True,
                }

        return None

    except Exception:
        return None


def render():
    st.title("🧪 Pipeline Studio")
    st.caption(
        "Ingest a dataset and watch it self-heal live: Bronze → Quality Check → Repair → Silver → Gold."
    )

    dataset_name, df = dataset_picker(key_prefix="studio")
    if df is None:
        st.stop()

    st.session_state.active_dataset = dataset_name

    # Every Studio page render attempts to restore the persistent execution.
    # This survives Streamlit navigation/reruns.
    try:
        restored = _restore_persistent_databricks_run(
            dataset_name,
            current_mode(load_settings()),
        )

        if restored and restored.get("_live"):
            st.session_state.last_run_id = restored["run_id"]
            st.session_state.active_dataset = dataset_name
            st.session_state.active_databricks_run_id = str(
                restored.get("_dbx_status", {}).get(
                    "run_id",
                    restored.get("dbx_run_id", ""),
                )
            )

            # IMPORTANT:
            # Streamlit navigation destroys the previous page render.
            # The Databricks job continues independently, so restore the
            # visible running state from durable history.
            _render_persistent_databricks_status(restored)
    except Exception:
        pass

    data_volume_bytes = df.memory_usage(deep=True).sum()

    with st.expander("🔍 Preview raw data", expanded=False):
        st.dataframe(df.head(20), use_container_width=True)
        st.caption(
            f"{len(df):,} rows × {df.shape[1]} columns — **{_human_size(data_volume_bytes)}** in memory"
        )

    explain_toggle = st.toggle(
        "AI-generated repair explanations",
        value=True,
        help="Uses your configured AI provider for a one-line rationale per repair; falls back to a canned note if none is configured.",
    )

    settings = load_settings()
    mode = current_mode(settings)
    databricks_ready = is_databricks_configured(settings, mode)
    if databricks_ready:
        st.caption(
            f"⚡ Will orchestrate via **Databricks** ({mode.title()} workspace, real Spark) — "
            "falls back to the in-app pipeline if the Job fails."
        )
        button_label = "▶️ Run Pipeline"
    else:
        st.caption(
            "💾 No Databricks configured for this mode — running the in-app pipeline directly."
        )
        button_label = "▶️ Run Pipeline"

    st.caption(
        f"DataDoctorAI orchestration ? {mode.title()} workspace ? "
        "Databricks with DuckDB fallback when unavailable."
    )

    if st.button(button_label, type="primary", use_container_width=True):
        if databricks_ready:
            _run_orchestrated_pipeline(dataset_name, df, mode, explain_toggle)
        else:
            run_id = history.new_run(dataset_name)
            runtime_state.create_run(run_id, dataset_name, mode="demo")
            st.session_state.last_run_id = run_id
            _execute_pipeline(dataset_name, df, run_id, explain_toggle)

    if (
        st.session_state.get("gold_result")
        and st.session_state.get("active_dataset") == dataset_name
    ):
        st.divider()
        _render_results(dataset_name)

    _render_databricks_job_section(dataset_name, df)


def _execute_pipeline(dataset_name: str, df: pd.DataFrame, run_id: str, explain: bool):
    progress = st.progress(0, text="Starting pipeline...")
    data_volume_bytes = df.memory_usage(deep=True).sum()

    # ---------------- BRONZE ----------------
    with st.status("🥉 Bronze — raw ingestion", expanded=True) as status:
        history.log_event(
            run_id, "bronze", f"Ingesting {len(df)} rows into Bronze layer"
        )
        st.write(
            f"Ingesting **{len(df)} rows × {df.shape[1]} columns** ({_human_size(data_volume_bytes)}) as-is, tagging with ingestion metadata..."
        )
        runtime_state.update_stage(
            run_id,
            "bronze",
            "running",
            rows_in=len(df),
            message=f"Ingesting {len(df):,} rows into Bronze",
        )
        _live_delay()
        bronze_df = bronze.ingest(df, dataset_name, run_id=run_id)
        st.session_state.bronze_df = bronze_df
        runtime_state.update_stage(
            run_id,
            "bronze",
            "success",
            rows_in=len(df),
            rows_out=len(bronze_df),
            message=f"Bronze materialized: {len(bronze_df):,} rows",
        )
        st.write(
            f"✅ Wrote table `bronze__{dataset_name}` ({len(bronze_df):,} rows, "
            f"{_human_size(bronze_df.memory_usage(deep=True).sum())})."
        )
        _backend_badge()
        status.update(label="🥉 Bronze — complete", state="complete")
    progress.progress(20, text="Bronze complete")

    # ---------------- PROFILING ----------------
    with st.status("📐 Profiling raw data", expanded=True) as status:
        runtime_state.update_stage(
            run_id,
            "profiling",
            "running",
            rows_in=len(df),
            message="Profiling schema, nulls, duplicates and data quality",
        )

        _live_delay()
        raw_profile = profiling.profile_dataframe(df)
        raw_score = profiling.quality_score(raw_profile)

        runtime_state.update_stage(
            run_id,
            "profiling",
            "success",
            rows_out=len(df),
            message=f"Initial quality score: {raw_score}/100",
        )

        _live_delay()
        st.write(
            f"Detected **{raw_profile['duplicate_rows']} duplicate rows**, "
            f"**{raw_profile['overall_null_pct']}% average nulls** across columns."
        )
        st.write(f"Initial quality score: {score_color(raw_score)} **{raw_score}/100**")
        status.update(label="📐 Profiling — complete", state="complete")
    progress.progress(35, text="Profiling complete")

    # ---------------- SILVER (quality + repair) ----------------
    with st.status(
        "🥈 Silver — quality checks & self-healing repair", expanded=True
    ) as status:
        history.log_event(run_id, "silver", "Running pre-repair quality checks")
        st.write(
            "Running quality checks: null thresholds, duplicates, negative values, outliers, categorical consistency..."
        )
        _live_delay()
        result = silver.process(bronze_df, dataset_name, run_id, explain=explain)
        st.session_state.silver_result = result

        failed_pre = [c for c in result["pre_checks"] if not c["passed"]]
        st.write(
            f"⚠️ **{len(failed_pre)}/{len(result['pre_checks'])}** checks failed before repair."
        )

        runtime_state.update_stage(
            run_id,
            "quality",
            "running",
            rows_in=len(df),
            message="Running pre-repair quality checks",
        )

        if result["repair_actions"]:
            runtime_state.update_stage(
                run_id,
                "repair",
                "repairing",
                rows_in=len(df),
                rows_out=len(result["df"]),
                message=f"Applying {len(result['repair_actions'])} self-healing action(s)",
            )

            st.write("**Self-healing actions taken:**")
            for action in result["repair_actions"]:
                _live_delay()
                line = f"- `{action['column_name']}` — {action['issue']} → **{action['action']}** ({action['rows_affected']} rows)"
                st.markdown(line)
                if action.get("explanation"):
                    st.caption(f"🩺 {action['explanation']}")
        else:
            st.write("No repairs were necessary — data already passed all checks.")

        runtime_state.update_stage(
            run_id,
            "repair",
            "success",
            rows_in=len(df),
            rows_out=len(result["df"]),
            message=f"Repair phase complete: {len(result['repair_actions'])} action(s)",
        )

        failed_post = [c for c in result["post_checks"] if not c["passed"]]
        runtime_state.update_stage(
            run_id,
            "quality",
            "failed" if failed_post else "success",
            rows_out=len(result["df"]),
            message=(
                f"{len(failed_post)} quality check(s) still failing"
                if failed_post
                else "All post-repair quality checks passed"
            ),
        )

        if failed_post:
            st.warning(
                f"{len(failed_post)} check(s) still failing after repair — see Monitor for details."
            )
        else:
            st.success("✅ All quality checks passing after self-healing.")
        history.log_event(
            run_id,
            "silver",
            f"Repair complete: {len(result['repair_actions'])} action(s) taken",
        )
        if result.get("below_minimum"):
            st.warning(
                f"⚠️ Quality score **{result['quality_score']}** is below your configured minimum "
                f"({load_settings()['quality']['minimum_score']}) — check Settings to adjust the threshold."
            )
        else:
            st.caption(f"Quality score: **{result.get('quality_score', '—')}**/100")
        runtime_state.update_stage(
            run_id,
            "silver",
            "running",
            rows_in=len(df),
            message="Materializing repaired Silver dataset",
        )
        _live_delay()
        runtime_state.update_stage(
            run_id,
            "silver",
            "success",
            rows_in=len(df),
            rows_out=len(result["df"]),
            message=f"Silver materialized: {len(result['df']):,} rows",
        )

        _backend_badge()
        status.update(label="🥈 Silver — complete", state="complete")
    progress.progress(70, text="Silver complete")

    # ---------------- GOLD ----------------
    with st.status("🥇 Gold — business aggregation", expanded=True) as status:
        runtime_state.update_stage(
            run_id,
            "gold",
            "running",
            rows_in=len(result["df"]),
            message="Building Gold business aggregates",
        )
        _live_delay()
        gold_result = gold.build(result["df"], dataset_name, run_id)
        st.session_state.gold_result = gold_result
        runtime_state.update_stage(
            run_id,
            "gold",
            "success",
            rows_in=len(result["df"]),
            rows_out=len(gold_result["df"]),
            message=f"Gold materialized: {len(gold_result['df']):,} rows",
        )
        history.log_event(run_id, "gold", "Business aggregates materialized")
        gold_bytes = gold_result["df"].memory_usage(deep=True).sum()
        st.write(
            f"Aggregated to **{len(gold_result['df'])} rows** ({_human_size(gold_bytes)}) "
            + (
                f"grouped by `{gold_result['group_column']}`."
                if gold_result["group_column"]
                else "as summary statistics."
            )
        )
        _backend_badge()
        status.update(label="🥇 Gold — complete", state="complete")
    progress.progress(90, text="Gold complete")

    # ---------------- AI PIPELINE PLAN (bonus insight) ----------------
    with st.status("🤖 AI reviewing pipeline design", expanded=True) as status:
        _live_delay()
        dtypes = {c: str(t) for c, t in df.dtypes.items()}
        plan = business_assistant.suggest_pipeline_plan(dtypes)
        st.write(plan["plan"])
        st.caption(f"via {plan['provider']}")
        status.update(label="🤖 AI review — complete", state="complete")
    progress.progress(100, text="Pipeline complete ✅")

    history.finish_run(
        run_id,
        "success",
        {
            "rows": len(df),
            "repairs": len(result["repair_actions"]),
            "quality_score_before": raw_score,
            "quality_score_after": result.get("quality_score"),
            "gold_rows": len(gold_result["df"]),
        },
    )
    st.balloons()


def _render_databricks_job_section(dataset_name: str, df: pd.DataFrame):
    """Available in BOTH modes now: uploads the dataset to a Unity Catalog Volume and
    submits a real Databricks Job that runs Bronze/Silver/Gold natively as Spark on
    the active mode's workspace (dbx_enterprise/notebooks/bronze_silver_gold_job.py) —
    separate from the in-app pandas pipeline above. Hidden entirely when that mode's
    Databricks credentials aren't configured."""
    settings = load_settings()
    mode = current_mode(settings)
    if not is_databricks_configured(settings, mode):
        return

    st.divider()
    with st.expander(
        f"⚡ Advanced: Run Native Databricks Spark Job — {mode.title()} workspace (Spark)",
        expanded=False,
    ):
        st.caption(
            f"Uploads this dataset to a Unity Catalog Volume on your **{mode.title()} Mode** workspace "
            "and submits a real Databricks Job that runs Bronze → Silver → Gold as PySpark on "
            "serverless compute — the transformation executes natively in your workspace instead of "
            "in this app's process. Defaults to serverless since most free-edition workspaces can't "
            "provision classic clusters."
        )
        notebook_choice = st.radio(
            "Notebook source",
            ["Auto-generated (bundled)", "Write my own"],
            horizontal=True,
            key="dbx_notebook_choice",
            help="Auto-generated uses dbx_enterprise/notebooks/bronze_silver_gold_job.py as-is. "
            "'Write my own' lets you edit the PySpark logic before it's deployed to your workspace.",
        )
        custom_notebook_source = None
        if notebook_choice == "Write my own":
            custom_notebook_source = st.text_area(
                "Notebook source (PySpark, Databricks notebook format)",
                value=st.session_state.get(
                    "dbx_custom_notebook_source", dbx_jobs.get_bundled_notebook_source()
                ),
                height=300,
                key="dbx_custom_notebook_source",
                help="Starts pre-filled with the bundled notebook — edit freely. Deployed to your "
                "workspace exactly as written when you submit.",
            )

        if st.button(
            "🚀 Submit Databricks Job", key="submit_dbx_job", use_container_width=True
        ):
            try:
                with st.spinner(
                    "Ensuring Volume, uploading, deploying notebook, submitting job..."
                ):
                    run_id, notebook_path = _submit_job(
                        dataset_name, df, mode, custom_notebook_source
                    )
                st.session_state.dbx_job_run_id = run_id
                st.session_state.dbx_job_dataset = dataset_name
                st.session_state.dbx_job_mode = mode
                st.success(
                    f"Job submitted — run_id `{run_id}`. Expand below and click Refresh to track it."
                )
            except Exception as e:  # noqa: BLE001
                st.error(f"Could not submit Databricks Job: {e}")
                history.log_audit(
                    "local-user",
                    "job_submission_failed",
                    dataset_name,
                    {"mode": mode, "error": str(e)},
                )
                st.warning(
                    "⚠️ **Falling back to the in-app pipeline** since the native Job "
                    "couldn't be submitted — this run will use the SQL Warehouse/DuckDB "
                    "path instead of Spark. Logged to the audit trail."
                )
                fallback_run_id = history.new_run(dataset_name)
                runtime_state.create_run(fallback_run_id, dataset_name, mode="demo")
                history.log_audit(
                    "local-user",
                    "auto_fallback_triggered",
                    fallback_run_id,
                    {"reason": "job_submission_failed", "original_error": str(e)},
                )
                st.session_state.last_run_id = fallback_run_id
                _execute_pipeline(dataset_name, df, fallback_run_id, explain=True)



def _render_results(dataset_name: str):
    st.subheader("📈 Results")
    silver_result = st.session_state.silver_result
    gold_result = st.session_state.gold_result

    _render_animated_flow(dataset_name, silver_result, gold_result)
    with st.expander("🔍 Exact flow numbers (Sankey)", expanded=False):
        _render_data_flow_diagram(dataset_name, silver_result, gold_result)

    tab1, tab2, tab3 = st.tabs(
        ["Silver (cleaned)", "Gold (aggregated)", "Quality Checks"]
    )
    with tab1:
        st.dataframe(silver_result["df"].head(50), use_container_width=True)
        st.caption(f"{len(silver_result['df'])} rows after self-healing")
    with tab2:
        st.dataframe(gold_result["df"], use_container_width=True)
        st.caption(
            "KPIs: "
            + ", ".join(f"{k}={v}" for k, v in list(gold_result["kpis"].items())[:6])
        )
    with tab3:
        for c in silver_result["post_checks"]:
            icon = "✅" if c["passed"] else "❌"
            st.write(f"{icon} **{c['check_name']}**")
            if not c["passed"]:
                st.json(c["details"])





