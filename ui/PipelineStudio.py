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
from ui import live_flow


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
    bronze_tbl = dbx_connection.read_table("bronze", dataset_name, mode=mode)
    silver_tbl = dbx_connection.read_table("silver", dataset_name, mode=mode)
    gold_tbl = dbx_connection.read_table("gold", dataset_name, mode=mode)

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
    st.session_state.last_run_id = run_id
    st.session_state["_dbx_result_dataset"] = dataset_name

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
    st.session_state["_active_dbx_internal_run_id"] = internal_run_id

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


def _get_databricks_stage_status(run_id: str, mode: str):
    """Read authoritative Databricks stage state for every pipeline stage."""

    try:
        from dbx_enterprise import connection
        from config.settings import get_databricks_config, load_settings

        settings = load_settings()
        cfg = get_databricks_config(settings, mode)

        catalog = cfg.get("catalog", "main")
        schema = cfg.get("schema", "default")

        safe_run_id = str(run_id).replace("'", "''")

        sql = f"""
            SELECT
                run_id,
                dataset,
                stage,
                state,
                rows_in,
                rows_out,
                message,
                updated_at
            FROM `{catalog}`.`{schema}`.`datadoctor_pipeline_status`
            WHERE run_id = '{safe_run_id}'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY stage
                ORDER BY updated_at DESC
            ) = 1
            ORDER BY updated_at ASC
        """

        result = connection.run_sql(sql, mode=mode)

        if result is None:
            return []

        if hasattr(result, "to_dict"):
            return result.to_dict("records")

        if isinstance(result, list):
            return result

        return []

    except Exception:
        return []


def _sync_databricks_runtime_state(restored: dict) -> bool:
    """Project authoritative Databricks stage rows into the shared runtime state.

    Dashboard and Studio then render the exact same live_flow component. The
    Databricks status Delta table remains authoritative; runtime_state is only
    the UI projection used by live_flow.
    """
    run_id = str(restored.get("run_id") or "")
    if not run_id:
        return False

    summary = restored.get("summary") or {}
    dataset = str(restored.get("dataset") or summary.get("dataset") or "")
    mode = str(summary.get("mode") or restored.get("mode") or "demo")
    if not dataset:
        return False

    try:
        current = runtime_state.get_run(run_id)
        if current is None:
            current = runtime_state.create_run(run_id, dataset, mode=mode)

        rows = _get_databricks_stage_status(run_id, mode)
        if not rows:
            # Keep the existing runtime state during a brief status-table lag.
            return runtime_state.get_run(run_id) is not None

        for row in rows:
            stage = str(row.get("stage") or "").strip().lower()
            if stage not in runtime_state.STAGES:
                continue
            state = str(row.get("state") or "waiting").strip().lower()
            if state not in runtime_state.STATES:
                state = "waiting"
            runtime_state.update_stage(
                run_id,
                stage,
                state,
                rows_in=int(row.get("rows_in") or 0),
                rows_out=int(row.get("rows_out") or 0),
                message=str(row.get("message") or ""),
                backend="databricks",
            )

        # The remote lifecycle is terminal only after the authoritative stage
        # table has been projected. This prevents a completed run flashing back
        # to RUNNING in the shared UI.
        status = str(restored.get("status") or "").lower()
        if status == "success":
            runtime_state.finish_run(
                run_id, "success", "Databricks Spark execution completed"
            )
        elif status == "failed":
            runtime_state.finish_run(
                run_id, "failed", "Databricks Spark execution failed"
            )

        return True
    except Exception:
        return False


def _render_databricks_live_flow(restored: dict) -> None:
    """Render the same stage-wise live UI used by Dashboard."""
    run_id = str(restored.get("run_id") or "")
    if _sync_databricks_runtime_state(restored):
        live_flow.render(run_id)


def _render_persistent_databricks_status(restored: dict):
    """Render a premium navigation-safe Databricks execution view."""

    import json

    summary = restored.get("summary") or {}

    if isinstance(summary, str):
        try:
            summary = json.loads(summary)
        except Exception:
            summary = {}

    dbx_status = restored.get("_dbx_status") or {}

    dbx_run_id = str(dbx_status.get("run_id") or summary.get("dbx_run_id") or "")

    lifecycle = str(dbx_status.get("life_cycle_state") or "RUNNING")

    result_state = str(dbx_status.get("result_state") or "")

    state_message = str(dbx_status.get("state_message") or "")

    lifecycle_upper = lifecycle.upper()
    terminal_success = (
        lifecycle_upper in ("TERMINATED", "SKIPPED")
        and result_state.upper() == "SUCCESS"
    )
    terminal_failure = (
        lifecycle_upper in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR")
        and not terminal_success
    )

    # --------------------------------------------------------
    # AUTHORITATIVE LIVE STAGE STATE
    #
    # Do NOT infer the active stage from Databricks lifecycle.
    # Databricks lifecycle only tells us whether the remote job
    # is running/finished. The stage-status table tells us which
    # actual pipeline stage is running.
    # --------------------------------------------------------
    # datadoctor_pipeline_status.run_id stores the internal DataDoctor
    # pipeline run ID, not the Databricks job/run ID.
    stage_rows = _get_databricks_stage_status(
        str(restored.get("run_id") or ""),
        summary.get("mode") or restored.get("mode") or "demo",
    )

    stage_order = (
        "bronze",
        "profiling",
        "quality",
        "repair",
        "silver",
        "gold",
    )

    stage_state_map = {}

    for row in stage_rows:
        stage = str(row.get("stage") or "").strip().lower()

        if stage not in stage_order:
            continue

        stage_state_map[stage] = {
            "state": str(row.get("state") or "waiting").strip().lower(),
            "rows_in": int(row.get("rows_in") or 0),
            "rows_out": int(row.get("rows_out") or 0),
            "message": str(row.get("message") or ""),
            "updated_at": row.get("updated_at"),
        }

    # Terminal SUCCESS only means the remote Spark job terminated successfully.
    # Individual stage states remain authoritative from datadoctor_pipeline_status.
    if terminal_success:
        completed_run = all(
            stage_state_map.get(stage, {}).get("state") == "success"
            for stage in stage_order
        )

        active_stage = None

        for stage in stage_order:
            state = stage_state_map.get(stage, {}).get("state")
            if state in ("running", "repairing", "retrying"):
                active_stage = stage
                completed_run = False
                break

    elif terminal_failure:
        completed_run = False
        active_stage = None

        for stage in stage_order:
            state = stage_state_map.get(stage, {}).get("state")
            if state in ("failed", "running", "repairing", "retrying"):
                active_stage = stage
                break

        if active_stage is None:
            active_stage = "quality"

    else:
        completed_run = False
        active_stage = None

        for stage in stage_order:
            state = stage_state_map.get(stage, {}).get("state")
            if state in ("running", "repairing", "retrying"):
                active_stage = stage
                break

        if active_stage is None and not stage_state_map:
            active_stage = "bronze"

    def _stage_css(stage):
        if completed_run:
            return "complete"

        info = stage_state_map.get(stage, {})
        state = str(info.get("state") or "waiting").lower()

        if state in ("running", "repairing", "retrying"):
            return "active"

        if state == "success":
            return "complete"

        if state == "failed":
            return "failed"

        return "waiting"

    st.divider()

    st.html(
        f"""
        <style>

        * {{
            box-sizing: border-box;
        }}

        body {{
            margin: 0;
            background: transparent;
            font-family:
                Inter,
                ui-sans-serif,
                system-ui,
                -apple-system,
                BlinkMacSystemFont,
                "Segoe UI",
                sans-serif;
        }}

        .dd-shell {{
            position: relative;
            width: 100%;
            min-height: 245px;
            padding: 22px 20px 18px;
            overflow: hidden;
            border: 1px solid rgba(148,163,184,.20);
            border-radius: 22px;
            background:
                radial-gradient(
                    circle at 15% 20%,
                    rgba(56,189,248,.12),
                    transparent 28%
                ),
                radial-gradient(
                    circle at 85% 80%,
                    rgba(168,85,247,.12),
                    transparent 30%
                ),
                linear-gradient(
                    135deg,
                    rgba(15,23,42,.88),
                    rgba(30,41,59,.72)
                );
            box-shadow:
                0 18px 45px rgba(0,0,0,.22),
                inset 0 1px 0 rgba(255,255,255,.08);
            backdrop-filter: blur(18px);
        }}

        .dd-shell::before {{
            content: "";
            position: absolute;
            width: 280px;
            height: 280px;
            left: -100px;
            top: -170px;
            border-radius: 50%;
            background: rgba(56,189,248,.08);
            filter: blur(45px);
            animation: dd-float 7s ease-in-out infinite;
        }}

        .dd-shell::after {{
            content: "";
            position: absolute;
            width: 260px;
            height: 260px;
            right: -90px;
            bottom: -160px;
            border-radius: 50%;
            background: rgba(168,85,247,.08);
            filter: blur(45px);
            animation: dd-float 8s ease-in-out infinite reverse;
        }}

        @keyframes dd-float {{
            0%,100% {{
                transform: translate3d(0,0,0) scale(1);
            }}
            50% {{
                transform: translate3d(35px,18px,0) scale(1.12);
            }}
        }}

        .dd-top {{
            position: relative;
            z-index: 2;
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 22px;
        }}

        .dd-brand {{
            display: flex;
            align-items: center;
            gap: 11px;
        }}

        .dd-live-dot {{
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #22c55e;
            box-shadow:
                0 0 0 4px rgba(34,197,94,.10),
                0 0 18px rgba(34,197,94,.75);
        }}

        .dd-live-dot.running {{
            animation: dd-pulse 1.5s ease-in-out infinite;
        }}

        .dd-live.completed {{
            border-color: rgba(34,197,94,.25);
            background: rgba(34,197,94,.06);
            color: #86efac;
        }}

        @keyframes dd-pulse {{
            0%,100% {{
                transform: scale(.8);
                opacity: .65;
            }}
            50% {{
                transform: scale(1.15);
                opacity: 1;
            }}
        }}

        .dd-title {{
            color: #f8fafc;
            font-size: 14px;
            font-weight: 700;
            letter-spacing: .02em;
        }}

        .dd-subtitle {{
            margin-top: 3px;
            color: #94a3b8;
            font-size: 11px;
        }}

        .dd-live {{
            padding: 6px 11px;
            border: 1px solid rgba(34,197,94,.25);
            border-radius: 999px;
            background: rgba(34,197,94,.08);
            color: #86efac;
            font-size: 10px;
            font-weight: 700;
            letter-spacing: .08em;
        }}

        .dd-pipeline {{
            position: relative;
            z-index: 2;
            display: flex;
            align-items: center;
            width: 100%;
        }}

        .dd-stage {{
            position: relative;
            flex: 1;
            min-width: 58px;
            text-align: center;
        }}

        .dd-icon {{
            position: relative;
            width: 48px;
            height: 48px;
            margin: auto;
            display: flex;
            align-items: center;
            justify-content: center;
            border: 1px solid rgba(148,163,184,.22);
            border-radius: 15px;
            background: rgba(15,23,42,.72);
            color: #64748b;
            font-size: 15px;
            font-weight: 800;
            box-shadow:
                inset 0 1px 0 rgba(255,255,255,.05),
                0 8px 20px rgba(0,0,0,.16);
            transition: all .35s ease;
        }}

        .dd-stage.complete .dd-icon {{
            border-color: rgba(34,197,94,.42);
            background: rgba(34,197,94,.10);
            color: #86efac;
        }}

        .dd-stage.active .dd-icon {{
            border-color: rgba(56,189,248,.65);
            background:
                radial-gradient(
                    circle,
                    rgba(56,189,248,.20),
                    rgba(15,23,42,.78)
                );
            color: #7dd3fc;
            box-shadow:
                0 0 0 5px rgba(56,189,248,.06),
                0 0 28px rgba(56,189,248,.28),
                inset 0 1px 0 rgba(255,255,255,.10);
            animation: dd-active 1.8s ease-in-out infinite;
        }}

        @keyframes dd-active {{
            0%,100% {{
                transform: translateY(0);
            }}
            50% {{
                transform: translateY(-4px);
            }}
        }}

        .dd-stage.active .dd-icon::before {{
            content: "";
            position: absolute;
            inset: -7px;
            border: 1px solid rgba(56,189,248,.25);
            border-radius: 19px;
            animation: dd-ring 2s ease-out infinite;
        }}

        @keyframes dd-ring {{
            0% {{
                transform: scale(.75);
                opacity: .8;
            }}
            100% {{
                transform: scale(1.3);
                opacity: 0;
            }}
        }}

        .dd-name {{
            margin-top: 9px;
            color: #cbd5e1;
            font-size: 9px;
            font-weight: 800;
            letter-spacing: .06em;
        }}

        .dd-detail {{
            margin-top: 3px;
            color: #64748b;
            font-size: 8px;
            letter-spacing: .05em;
        }}

        .dd-connector {{
            position: relative;
            flex: .55;
            height: 2px;
            margin: 0 3px;
            overflow: hidden;
            background: rgba(100,116,139,.25);
        }}

        .dd-connector::after {{
            content: "";
            position: absolute;
            top: 0;
            left: -35%;
            width: 35%;
            height: 100%;
            background:
                linear-gradient(
                    90deg,
                    transparent,
                    #38bdf8,
                    transparent
                );
            box-shadow: 0 0 10px #38bdf8;
            opacity: 0;
        }}

        .dd-connector.flowing::after {{
            opacity: 1;
            animation: dd-flow 1.5s linear infinite;
        }}

        @keyframes dd-flow {{
            from {{
                left: -35%;
            }}
            to {{
                left: 110%;
            }}
        }}

        .dd-footer {{
            position: relative;
            z-index: 2;
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 24px;
            padding-top: 12px;
            border-top: 1px solid rgba(148,163,184,.10);
            color: #64748b;
            font-size: 9px;
        }}

        .dd-footer-live {{
            display: flex;
            align-items: center;
            gap: 7px;
            color: #94a3b8;
        }}

        .dd-mini {{
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #22c55e;
            box-shadow: 0 0 9px rgba(34,197,94,.8);
        }}

        </style>

        <div class="dd-shell">

            <div class="dd-top">

                <div class="dd-brand">
                    <span class="dd-live-dot {'running' if not completed_run else ''}"></span>

                    <div>
                        <div class="dd-title">
                            DataDoctor AI · {'Pipeline Complete' if completed_run else 'Live Pipeline'}
                        </div>

                        <div class="dd-subtitle">
                            {'Remote Spark execution · completed successfully' if completed_run else 'Remote Spark execution · live stage tracking'}
                        </div>
                    </div>
                </div>

                <div class="dd-live {'completed' if completed_run else ''}">
                    {'✓ COMPLETED' if completed_run else '● LIVE'}
                </div>

            </div>

            <div class="dd-pipeline">

                <div class="dd-stage complete">
                    <div class="dd-icon">↥</div>
                    <div class="dd-name">UPLOADED</div>
                    <div class="dd-detail">SOURCE</div>
                </div>

                <div class="dd-connector"></div>

                <div class="dd-stage {_stage_css("bronze")}">
                    <div class="dd-icon">B</div>
                    <div class="dd-name">BRONZE</div>
                    <div class="dd-detail">INGEST</div>
                </div>

                <div class="dd-connector"></div>

                <div class="dd-stage {'active' if active_stage == 'profiling' else ('complete' if completed_run or active_stage in ('quality','repair','silver','gold') else '')}">
                    <div class="dd-icon">P</div>
                    <div class="dd-name">PROFILING</div>
                    <div class="dd-detail">ANALYZE</div>
                </div>

                <div class="dd-connector"></div>

                <div class="dd-stage {'active' if active_stage == 'quality' else ('complete' if completed_run or active_stage in ('repair','silver','gold') else '')}">
                    <div class="dd-icon">Q</div>
                    <div class="dd-name">QUALITY</div>
                    <div class="dd-detail">VALIDATE</div>
                </div>

                <div class="dd-connector"></div>

                <div class="dd-stage {'active' if active_stage == 'repair' else ('complete' if completed_run or active_stage in ('silver','gold') else '')}">
                    <div class="dd-icon">AI</div>
                    <div class="dd-name">AI REPAIR</div>
                    <div class="dd-detail">REMEDIATE</div>
                </div>

                <div class="dd-connector"></div>

                <div class="dd-stage {'active' if active_stage == 'silver' else ('complete' if completed_run or active_stage == 'gold' else '')}">
                    <div class="dd-icon">S</div>
                    <div class="dd-name">SILVER</div>
                    <div class="dd-detail">CLEAN</div>
                </div>

                <div class="dd-connector"></div>

                <div class="dd-stage {'active' if active_stage == 'gold' else ('complete' if completed_run else '')}">
                    <div class="dd-icon">G</div>
                    <div class="dd-name">GOLD</div>
                    <div class="dd-detail">SERVE</div>
                </div>

            </div>

            <div class="dd-footer">

                <div class="dd-footer-live">
                    <span class="dd-mini"></span>
                    <span>
                        {'Databricks Spark execution completed' if completed_run else 'Databricks Spark execution active'}
                    </span>
                </div>

                <span>Run {dbx_run_id}</span>

            </div>

        </div>
        """,
    )

    if lifecycle_upper in ("TERMINATED", "SKIPPED"):
        if result_state.upper() == "SUCCESS":
            st.success(f"Databricks run `{dbx_run_id}` completed successfully.")
        else:
            st.warning(
                f"Databricks run `{dbx_run_id}` finished with state "
                f"`{result_state or lifecycle}`."
            )

    if state_message:
        st.caption(state_message)

    run_page_url = dbx_status.get("run_page_url") or summary.get(
        "databricks_run_page_url"
    )

    if run_page_url:
        st.link_button(
            "Open Databricks Run",
            run_page_url,
        )


def _restore_persistent_databricks_run(dataset_name: str, mode: str):
    """Restore only the active DataDoctor Databricks run for this Studio session.

    Important UX rules:
    - Never poll a completed run every second.
    - Prefer the exact internal DataDoctor run started by this session.
    - If the page was navigated away from, recover the newest persisted RUNNING
      run for the selected dataset so execution remains visible.
    - A completed run is returned once when requested, but it never keeps the
      polling loop alive.
    """
    import json

    try:
        runs = history.get_runs(limit=25)
        target_internal_id = str(
            st.session_state.get("_active_dbx_internal_run_id") or ""
        )

        # First, restore the exact run this Studio session started.
        candidates = runs
        if target_internal_id:
            candidates = [
                r for r in runs if str(r.get("run_id") or "") == target_internal_id
            ]

        # If there is no exact session run, recover the newest persisted
        # RUNNING Databricks run globally.  While a real pipeline is active,
        # the active run owns the Studio execution area even if the user
        # changes the dataset selector.  This prevents the selector from
        # making a live run disappear.  Once the run becomes terminal, the
        # active identity is cleared and the selector is free again.
        if not candidates:
            candidates = [
                r for r in runs if str(r.get("status") or "").lower() == "running"
            ]

        for run in candidates:
            # An exact session run may have a different dataset because the
            # user changed the picker after starting it.  In that case it must
            # still be restored; otherwise only consider the selected dataset.
            if not target_internal_id and str(run.get("dataset") or "") != str(
                dataset_name
            ):
                continue

            persisted_status = str(run.get("status") or "").lower()
            if persisted_status not in {"running", "success", "failed"}:
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

            # Submission is still happening in the process-level worker. Keep
            # the fragment alive until the worker persists the remote run ID.
            if not dbx_run_id:
                if (
                    persisted_status == "running"
                    and str(run.get("run_id")) == target_internal_id
                ):
                    st.session_state.last_run_id = run["run_id"]
                    st.session_state["_active_run_dataset"] = str(run.get("dataset") or summary.get("dataset") or dataset_name)
                    return {
                        **run,
                        "summary": summary,
                        "_dbx_status": {
                            "run_id": "",
                            "life_cycle_state": "PENDING",
                            "result_state": "",
                            "state_message": "Submitting Databricks Spark job…",
                        },
                        "_live": True,
                    }
                continue

            run_mode = summary.get("mode") or mode
            st.session_state.last_run_id = run["run_id"]
            st.session_state["_active_run_dataset"] = str(run.get("dataset") or summary.get("dataset") or dataset_name)

            try:
                dbx_status = dbx_jobs.get_run_status(str(dbx_run_id), mode=run_mode)
            except Exception as exc:
                # Temporary connectivity problems must not convert a real run
                # into FAILED and must not trigger a full-page rerun.
                return {
                    **run,
                    "summary": summary,
                    "_dbx_status": {
                        "run_id": str(dbx_run_id),
                        "life_cycle_state": "RUNNING",
                        "result_state": "",
                        "state_message": f"Databricks status temporarily unavailable: {exc}",
                    },
                    "_live": persisted_status == "running",
                    "_dbx_unreachable": True,
                }

            lifecycle = str(dbx_status.get("life_cycle_state") or "").upper()
            result = str(dbx_status.get("result_state") or "").upper()

            if lifecycle in {"PENDING", "RUNNING"}:
                st.session_state.active_databricks_run_id = str(dbx_run_id)
                st.session_state["_active_dbx_internal_run_id"] = str(run["run_id"])
                return {
                    **run,
                    "summary": summary,
                    "_dbx_status": dbx_status,
                    "_live": True,
                }

            if lifecycle == "TERMINATED" and result == "SUCCESS":
                if persisted_status != "success":
                    history.finish_run(
                        run["run_id"],
                        "success",
                        {
                            **summary,
                            "result_state": result,
                            "life_cycle_state": lifecycle,
                            "databricks_run_page_url": dbx_status.get(
                                "run_page_url", ""
                            ),
                        },
                    )
                st.session_state.active_databricks_run_id = None
                st.session_state["_active_dbx_internal_run_id"] = None
                return {
                    **run,
                    "summary": summary,
                    "status": "success",
                    "_dbx_status": dbx_status,
                    "_live": False,
                }

            if lifecycle in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
                error = (
                    dbx_status.get("error_message")
                    or dbx_status.get("state_message")
                    or "Databricks job failed"
                )
                if persisted_status != "failed":
                    history.finish_run(
                        run["run_id"],
                        "failed",
                        {
                            **summary,
                            "reason": "databricks_job_failed",
                            "error": error,
                            "result_state": result,
                            "life_cycle_state": lifecycle,
                            "databricks_run_page_url": dbx_status.get(
                                "run_page_url", ""
                            ),
                        },
                    )
                st.session_state.active_databricks_run_id = None
                st.session_state["_active_dbx_internal_run_id"] = None
                return {
                    **run,
                    "summary": summary,
                    "status": "failed",
                    "_dbx_status": dbx_status,
                    "_live": False,
                }

        return None
    except Exception:
        return None


def _render_live_databricks_region(dataset_name: str, mode: str):
    """Render the current persisted Databricks run state once.

    Databricks stage-status data is authoritative. This function does not
    schedule Streamlit reruns or start another Spark execution.
    """
    # A persisted active run always wins over the dataset picker.
    # This prevents navigation back to Studio from switching the live run
    # to the default dataset.
def _active_persistent_databricks_dataset(mode: str):
    """Return the dataset for the currently persisted active pipeline run."""
    try:
        from database import history

        for saved_run in history.get_runs(limit=100):
            status = str(saved_run.get("status") or "").strip().lower()

            if status not in ("running", "queued", "pending"):
                continue

            summary = saved_run.get("summary") or {}

            if isinstance(summary, str):
                try:
                    import json
                    summary = json.loads(summary)
                except Exception:
                    summary = {}

            if not isinstance(summary, dict):
                summary = {}

            dataset = str(
                saved_run.get("dataset")
                or summary.get("dataset")
                or ""
            ).strip()

            if dataset:
                return dataset

    except Exception:
        pass

    return None


    active_dataset = _active_persistent_databricks_dataset(mode)
    effective_dataset = active_dataset or dataset_name

    restored = _restore_persistent_databricks_run(
        effective_dataset,
        mode,
    )

    if not restored:
        return False

    run_id = str(restored["run_id"])
    summary = restored.get("summary") or {}
    if not isinstance(summary, dict):
        summary = {}

    # The history record is the authoritative identity of the DataDoctor
    # pipeline.  dbx_run_id is only the remote Databricks job identity.
    run_dataset = str(restored.get("dataset") or summary.get("dataset") or dataset_name)
    run_mode = str(summary.get("mode") or restored.get("mode") or mode)

    st.session_state.last_run_id = run_id
    st.session_state["_active_run_dataset"] = run_dataset

    dbx_status = restored.get("_dbx_status") or {}
    dbx_run_id = str(dbx_status.get("run_id") or summary.get("dbx_run_id") or "")
    st.session_state.active_databricks_run_id = dbx_run_id or None

    # Only show the execution flow when the selected dataset is the run dataset.
    # Selecting another domain must not replace that domain's data with the active run.
    if str(dataset_name).strip().lower() == str(run_dataset).strip().lower():
        _render_databricks_live_flow(restored)

    if str(restored.get("status") or "").lower() == "success" and str(dataset_name).strip().lower() == str(run_dataset).strip().lower():
        loaded_for = st.session_state.get("_dbx_results_loaded_for")
        if loaded_for != run_id:
            try:
                _populate_results_from_databricks(run_dataset, run_mode, run_id)
                st.session_state["_dbx_results_loaded_for"] = run_id
                st.session_state["_dbx_completed_restored"] = restored
            except Exception as exc:
                st.warning(
                    f"Databricks completed, but result tables are not readable yet: {exc}"
                )

        # Render completed results for the same dataset/run only.
        if (
            st.session_state.get("gold_result")
            and st.session_state.get("_dbx_results_loaded_for") == run_id
        ):
            st.session_state["_dbx_completed_restored"] = restored

        if (
            st.session_state.get("gold_result")
            and st.session_state.get("_dbx_results_loaded_for") == run_id
        ):
            st.divider()
            _render_results(run_dataset)

    return True


def _find_existing_processed_run(dataset_name: str, mode: str):
    """Return an already-successful native Databricks run for this dataset/mode.

    Run Pipeline is intentionally idempotent for a dataset: once a dataset has
    a successful Spark result, clicking Run Pipeline again must not submit a
    second Spark run or rewrite the same processed tables.
    """
    import json

    try:
        runs = history.get_runs(limit=100)
    except Exception:
        return None

    for run in runs:
        if str(run.get("dataset") or "") != str(dataset_name):
            continue
        if str(run.get("status") or "").lower() != "success":
            continue

        summary = run.get("summary") or {}
        if isinstance(summary, str):
            try:
                summary = json.loads(summary)
            except Exception:
                continue
        if not isinstance(summary, dict):
            continue

        if str(summary.get("engine") or "").lower() != "databricks":
            continue
        if str(summary.get("mode") or mode).lower() != str(mode).lower():
            continue
        if not summary.get("dbx_run_id"):
            continue

        return {**run, "summary": summary}

    return None


def _show_existing_processed_run(run: dict, dataset_name: str, mode: str):
    """Load/display existing Spark results without submitting another run."""
    run_id = str(run.get("run_id") or "")
    summary = run.get("summary") or {}
    run_dataset = str(summary.get("dataset") or run.get("dataset") or dataset_name)
    run_mode = str(summary.get("mode") or mode)

    st.session_state.last_run_id = run_id
    st.session_state["_active_run_dataset"] = run_dataset
    st.session_state["_dbx_completed_restored"] = run

    loaded_for = st.session_state.get("_dbx_results_loaded_for")
    if loaded_for != run_id:
        try:
            _populate_results_from_databricks(run_dataset, run_mode, run_id)
            st.session_state["_dbx_results_loaded_for"] = run_id
        except Exception as exc:
            st.warning(f"Existing Databricks results could not be loaded: {exc}")
            return

    st.info(
        f"This dataset is already processed successfully (run {summary.get('dbx_run_id', run_id)}). "
        "No new pipeline run was started."
    )


def render():
    st.title("🧪 Pipeline Studio")
    st.caption(
        "Ingest a dataset and watch it self-heal live: Bronze → Profiling → Quality → AI Repair → Silver → Gold."
    )

    # ------------------------------------------------------------
    # Navigation-safe active Databricks dataset.
    #
    # The persisted running pipeline wins over the picker's
    # default dataset. This prevents returning to Studio from
    # silently switching the UI back to another domain.
    # ------------------------------------------------------------
    mode = current_mode(load_settings())

    _persisted_active_dataset = None

    try:
        _persisted_active_dataset = (
            _active_persistent_databricks_dataset(mode)
        )
    except Exception:
        _persisted_active_dataset = None

    if _persisted_active_dataset:
        # If the picker uses a Streamlit session-state key,
        # preserve the active run's dataset for this page.
        try:
            st.session_state["studio_dataset"] = (
                _persisted_active_dataset
            )
        except Exception:
            pass

    dataset_name, df = dataset_picker(key_prefix="studio")

    if _persisted_active_dataset:
        # The persisted run is authoritative for the live execution
        # view. Do not allow a stale/default picker value to replace it.
        dataset_name = _persisted_active_dataset

    if df is None:
        st.stop()

    # Show the real active run dataset instead of silently reverting
    # the user to the picker's default while Spark is still running.
    try:
        _active_ui_dataset = _active_persistent_databricks_dataset(
            current_mode(load_settings())
        )

        if _active_ui_dataset:
            st.info(
                f"Currently running: **{_active_ui_dataset}** ? "
                "the live Databricks pipeline is preserved while you navigate."
            )
    except Exception:
        pass

    # The picker controls what a NEW run would execute.

    # All Databricks polling/results live in a scoped fragment. This is the
    # critical fix for the visible white-page refresh.
    has_persistent_dbx_run = _render_live_databricks_region(dataset_name, mode)

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

    databricks_ready = is_databricks_configured(load_settings(), mode)
    if databricks_ready:
        st.caption(
            f"⚡ Primary orchestration: **Databricks** ({mode.title()} workspace, real Spark) — "
            "the primary Run Pipeline path submits exactly one Databricks run."
        )
    else:
        st.caption(
            "💾 No Databricks configured for this mode — running the in-app pipeline directly."
        )

    if st.button("▶️ Run Pipeline", type="primary", use_container_width=True):
        # Never submit a second Spark run while one is already active.
        active_internal = str(st.session_state.get("_active_dbx_internal_run_id") or "")
        if databricks_ready and active_internal:
            st.info(
                "A Databricks pipeline is already running. Studio is showing its live status; "
                "no second run was started."
            )
        elif databricks_ready:
            # Every explicit Run Pipeline click starts a new run.
            # Only the active-run guard above prevents duplicate execution.
            _run_orchestrated_pipeline(dataset_name, df, mode, explain_toggle)
        else:
            existing_demo = None
            try:
                existing_demo = next(
                    (
                        r
                        for r in history.get_runs(limit=100)
                        if str(r.get("dataset") or "") == str(dataset_name)
                        and str(r.get("status") or "").lower() == "success"
                    ),
                    None,
                )
            except Exception:
                existing_demo = None

            if existing_demo:
                st.info(
                    "This dataset is already processed. No new pipeline run was started."
                )
            else:
                run_id = history.new_run(dataset_name)
                runtime_state.create_run(run_id, dataset_name, mode="demo")
                st.session_state.last_run_id = run_id
                _execute_pipeline(dataset_name, df, run_id, explain_toggle)

    # Local/demo execution keeps the existing result rendering. Native
    # Databricks results are rendered by the scoped fragment above.
    completed_restored = st.session_state.get("_dbx_completed_restored")
    if not has_persistent_dbx_run and completed_restored:
        _render_databricks_live_flow(completed_restored)

    result_dataset = st.session_state.get("_dbx_result_dataset")
    if (
        not has_persistent_dbx_run
        and st.session_state.get("gold_result")
        and result_dataset
        and str(result_dataset) == str(dataset_name)
    ):
        st.divider()
        _render_results(str(result_dataset))

    # Explicit/manual native path remains separate from primary Run Pipeline.
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

    # The live glass pipeline above is the only execution animation.
    # Results should remain static after completion; keep the Sankey for exact numbers.
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

