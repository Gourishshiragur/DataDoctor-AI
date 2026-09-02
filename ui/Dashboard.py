"""DataDoctor AI — modern operational dashboard."""

import html
import time

import pandas as pd
import streamlit as st

from ui.browser_live import render as render_browser_live

from config.settings import current_mode, load_settings
from database import history
from storage import router as db
from pipeline import runtime_state
from ui import live_flow

# ---------------------------------------------------------------------------
# Page styling
# ---------------------------------------------------------------------------


def _inject_css():
    st.markdown(
        """
        <style>
        .dd-shell {
            padding: 0.2rem 0 1.5rem 0;
        }

        .dd-hero {
            border: 1px solid rgba(148,163,184,.18);
            border-radius: 22px;
            padding: 24px 26px;
            margin-bottom: 18px;
            background:
                radial-gradient(circle at 85% 15%, rgba(59,130,246,.14), transparent 30%),
                linear-gradient(135deg, rgba(15,23,42,.98), rgba(17,24,39,.96));
            box-shadow: 0 12px 40px rgba(0,0,0,.18);
        }

        .dd-eyebrow {
            color: #60a5fa;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: .12em;
            text-transform: uppercase;
            margin-bottom: 6px;
        }

        .dd-title {
            color: #f8fafc;
            font-size: 30px;
            font-weight: 800;
            margin: 0;
        }

        .dd-subtitle {
            color: #94a3b8;
            font-size: 14px;
            margin-top: 7px;
        }

        .dd-mode {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            margin-top: 16px;
            padding: 7px 12px;
            border-radius: 999px;
            background: rgba(255,255,255,.06);
            border: 1px solid rgba(255,255,255,.10);
            color: #cbd5e1;
            font-size: 12px;
            font-weight: 600;
        }

        .dd-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
            background: #22c55e;
            box-shadow: 0 0 12px rgba(34,197,94,.7);
        }

        .dd-dot.demo {
            background: #f59e0b;
            box-shadow: 0 0 12px rgba(245,158,11,.65);
        }

        .dd-kpi {
            min-height: 116px;
            border: 1px solid rgba(148,163,184,.17);
            border-radius: 18px;
            padding: 18px;
            background: linear-gradient(
                145deg,
                rgba(30,41,59,.82),
                rgba(15,23,42,.88)
            );
            box-shadow: 0 8px 25px rgba(0,0,0,.12);
        }

        .dd-kpi-label {
            color: #94a3b8;
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 10px;
        }

        .dd-kpi-value {
            color: #f8fafc;
            font-size: 28px;
            font-weight: 800;
            line-height: 1;
        }

        .dd-kpi-meta {
            color: #64748b;
            font-size: 11px;
            margin-top: 10px;
        }

        .dd-section {
            color: #e2e8f0;
            font-size: 18px;
            font-weight: 750;
            margin: 24px 0 12px;
        }

        .flow {
            border: 1px solid rgba(148,163,184,.17);
            border-radius: 22px;
            padding: 22px 18px;
            background:
                radial-gradient(circle at 50% 0%, rgba(59,130,246,.08), transparent 45%),
                rgba(15,23,42,.78);
            overflow-x: auto;
        }

        .flow-row {
            display: flex;
            align-items: center;
            justify-content: center;
            min-width: 760px;
            gap: 0;
        }

        .incident-empty {
            color: #94a3b8;
            font-size: 13px;
            padding: 20px 0;
        }

        .incident-ok {
            display: flex;
            gap: 12px;
            align-items: center;
            color: #bbf7d0;
            font-size: 13px;
            padding: 20px 0;
        }

        .incident-icon {
            width: 36px;
            height: 36px;
            border-radius: 12px;
            display: grid;
            place-items: center;
            background: rgba(34,197,94,.10);
            border: 1px solid rgba(34,197,94,.18);
            font-size: 17px;
        }

        @media (max-width: 800px) {
            .dd-title { font-size: 24px; }
            .flow-row { justify-content: flex-start; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _status_counts(runs):
    success = sum(1 for r in runs if r.get("status") == "success")
    failed = sum(1 for r in runs if r.get("status") == "failed")
    running = sum(1 for r in runs if r.get("status") == "running")
    return success, failed, running



def _hydrate_databricks_results(run: dict, summary: dict, mode: str) -> bool:
    """Load completed native Databricks results into the current UI session.

    The Databricks Job is independent of Streamlit. Therefore the Dashboard
    must re-read the durable Bronze/Silver/Gold tables after the Job finishes
    instead of relying on Pipeline Studio session_state.
    """
    run_id = run.get("run_id")
    dataset_name = run.get("dataset")

    if not run_id or not dataset_name:
        return False

    # Do not repeatedly hit Databricks on every Streamlit rerun after success.
    if st.session_state.get("dashboard_dbx_hydrated_run_id") == run_id:
        return True

    try:
        from dbx_enterprise import connection as dbx_connection

        bronze_df = dbx_connection.read_table(
            "bronze",
            dataset_name,
            mode=mode,
        )
        silver_df = dbx_connection.read_table(
            "silver",
            dataset_name,
            mode=mode,
        )
        gold_df = dbx_connection.read_table(
            "gold",
            dataset_name,
            mode=mode,
        )

        # Shared session state for the Dashboard / other UI pages.
        st.session_state.bronze_df = bronze_df
        st.session_state.silver_result = {
            "df": silver_df,
            "pre_checks": [],
            "post_checks": [],
            "repair_actions": [],
            "quality_score": None,
            "below_minimum": False,
        }
        st.session_state.gold_result = {
            "df": gold_df,
            "kpis": {},
            "group_column": None,
        }
        st.session_state.active_dataset = dataset_name
        st.session_state.last_run_id = run_id

        # Dedicated Dashboard copy so Gold rendering does not depend on
        # storage.router's current mode or DuckDB table list.
        st.session_state.dashboard_dbx_gold = gold_df
        st.session_state.dashboard_dbx_gold_dataset = dataset_name
        st.session_state.dashboard_dbx_gold_run_id = run_id

        # Durable backend evidence.
        for layer in ("bronze", "silver", "gold"):
            try:
                history.log_backend_event(
                    run_id,
                    layer,
                    dataset_name,
                    "databricks",
                    "databricks",
                    False,
                    "",
                )
            except Exception:
                pass

        st.session_state.dashboard_dbx_hydrated_run_id = run_id
        return True

    except Exception as exc:
        # The Job may report SUCCESS slightly before all result tables become
        # queryable. Do not mark the run failed. Retry on the next Dashboard
        # rerun instead.
        st.session_state.dashboard_dbx_hydration_error = str(exc)
        return False



def _sync_persistent_databricks_runs():
    """Synchronize persisted Databricks executions with local history.

    Databricks terminal state is authoritative. Result hydration is
    best-effort and must never prevent a successful/failed Databricks
    execution from being finalized in SQLite.
    """
    try:
        import json
        from database import history
        from dbx_enterprise import jobs as dbx_jobs
        from pipeline import runtime_state

        runs = history.get_runs(limit=25)

        for run in runs:
            if str(run.get("status") or "").lower() != "running":
                continue

            summary = run.get("summary") or {}

            if isinstance(summary, str):
                try:
                    summary = json.loads(summary)
                except Exception:
                    summary = {}

            dbx_run_id = summary.get("dbx_run_id")
            mode = summary.get("mode")

            if not dbx_run_id or not mode:
                continue

            try:
                status = dbx_jobs.get_run_status(
                    str(dbx_run_id),
                    mode=mode,
                )
            except Exception:
                # A temporary Databricks API failure must not turn a
                # legitimately running job into FAILED.
                continue

            lifecycle = str(
                status.get("life_cycle_state") or ""
            ).upper()
            result = str(
                status.get("result_state") or ""
            ).upper()

            if lifecycle in ("PENDING", "RUNNING"):
                # The in-memory runtime can disappear when Streamlit
                # reruns or when the user navigates between pages.
                # Reconstruct it from durable SQLite history first.
                if runtime_state.get_run(run["run_id"]) is None:
                    runtime_state.recover_run(run["run_id"])

                if runtime_state.get_run(run["run_id"]) is not None:
                    runtime_state.update_stage(
                        run["run_id"],
                        "bronze",
                        "running",
                        message=(
                            f"Databricks Spark Job {dbx_run_id} ? "
                            f"{lifecycle}"
                        ),
                    )
                continue

            # ---------------------------------------------------------
            # TERMINAL SUCCESS
            #
            # Finalize SQLite FIRST. Hydration is optional and must
            # never be able to leave a completed Databricks run stuck
            # in RUNNING state.
            # ---------------------------------------------------------
            if lifecycle == "TERMINATED" and result == "SUCCESS":
                success_summary = {
                    **summary,
                    "result_state": result,
                    "life_cycle_state": lifecycle,
                    "databricks_run_page_url": status.get(
                        "run_page_url", ""
                    ),
                    "results_hydrated": False,
                }

                # Authoritative state transition.
                history.finish_run(
                    run["run_id"],
                    "success",
                    success_summary,
                )

                # Reconstruct the completed visual state from the
                # durable SQLite record. This is necessary because the
                # in-memory runtime may not exist after page navigation.
                runtime_state.recover_run(run["run_id"])

                if runtime_state.get_run(run["run_id"]) is not None:
                    for stage in (
                        "bronze",
                        "profiling",
                        "quality",
                        "repair",
                        "silver",
                        "gold",
                    ):
                        runtime_state.update_stage(
                            run["run_id"],
                            stage,
                            "success",
                            message="Completed by Databricks Spark Job",
                        )

                # Hydration is deliberately best-effort.
                try:
                    hydrated = _hydrate_databricks_results(
                        run,
                        summary,
                        mode,
                    )

                    if hydrated:
                        success_summary["results_hydrated"] = True
                        history.finish_run(
                            run["run_id"],
                            "success",
                            success_summary,
                        )

                except Exception as hydration_error:
                    success_summary["hydration_error"] = str(
                        hydration_error
                    )

                    # Keep the authoritative SUCCESS state while
                    # recording why optional result hydration failed.
                    try:
                        history.finish_run(
                            run["run_id"],
                            "success",
                            success_summary,
                        )
                    except Exception:
                        pass

                continue

            # ---------------------------------------------------------
            # TERMINAL FAILURE
            # ---------------------------------------------------------
            if lifecycle in (
                "TERMINATED",
                "SKIPPED",
                "INTERNAL_ERROR",
            ):
                error_message = (
                    f"Databricks Job {dbx_run_id}: "
                    f"{lifecycle} / "
                    f"{result or 'no result state'}"
                )

                detailed_error = (
                    status.get("error_message")
                    or status.get("state_message")
                    or ""
                )

                if detailed_error:
                    error_message += f" ? {detailed_error}"

                runtime_state.update_stage(
                    run["run_id"],
                    "bronze",
                    "failed",
                    message=error_message,
                )

                history.finish_run(
                    run["run_id"],
                    "failed",
                    {
                        **summary,
                        "reason": "databricks_job_failed",
                        "error": error_message,
                        "result_state": result,
                        "life_cycle_state": lifecycle,
                    },
                )

    except Exception as exc:
        # Dashboard must remain usable even if synchronization itself
        # encounters an unexpected error.
        print(
            "WARNING: Databricks synchronization failed:",
            repr(exc),
        )


def render():
    _sync_persistent_databricks_runs()
    # Reconcile abandoned local executions before calculating operational KPIs.
    # This prevents historical Streamlit interruptions from appearing forever
    # as active pipeline runs.
    try:
        history.reconcile_stale_runs(max_running_seconds=900)
    except Exception:
        # Dashboard availability must not depend on reconciliation.
        pass

    _inject_css()

    settings = load_settings()
    mode = current_mode(settings)

    # ========================================================
    # ALWAYS-MOUNTED LIVE PIPELINE MONITOR
    #
    # The browser component stays mounted even when no pipeline
    # is currently running. JavaScript discovers the active
    # internal DataDoctorAI run through /active and then polls
    # the real backend stage-status endpoint.
    #
    # No Streamlit rerun.
    # No fragment.
    # No page refresh.
    # ========================================================
    try:
        render_browser_live(
            "",
            mode,
            height=470,
        )
    except Exception:
        # Live monitor must never prevent Dashboard rendering.
        pass

    runs = history.get_runs(limit=100)

    total_runs = len(runs)
    success_runs, failed_runs, running_runs = _status_counts(runs)

    try:
        tables = db.list_tables("gold")
    except Exception:
        tables = []

    success_rate = round((success_runs / total_runs) * 100) if total_runs else 100

    health = max(0, min(100, success_rate))

    # ------------------------------------------------------------------
    # Hero
    # ------------------------------------------------------------------

    mode_label = "Enterprise / Databricks" if mode == "enterprise" else "Demo / DuckDB"
    mode_class = "" if mode == "enterprise" else "demo"

    st.markdown(
        f"""
        <div class="dd-shell">
            <div class="dd-hero">
                <div class="dd-eyebrow">Autonomous Data Reliability Platform</div>
                <div class="dd-title">DataDoctor AI</div>
                <div class="dd-subtitle">
                    Observe → Diagnose → Generate Fix → Repair → Verify
                </div>
                <div class="dd-mode">
                    <span class="dd-dot {mode_class}"></span>
                    {html.escape(mode_label)}
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------------
    # KPI cards
    # ------------------------------------------------------------------

    k1, k2, k3, k4 = st.columns(4)

    cards = [
        ("Pipeline Runs", f"{total_runs:,}", "execution history"),
        ("Success Rate", f"{success_rate}%", "pipeline reliability"),
        ("Active Runs", f"{running_runs:,}", "currently processing"),
        ("Gold Datasets", f"{len(tables):,}", "materialized outputs"),
    ]

    for col, (label, value, meta) in zip((k1, k2, k3, k4), cards):
        with col:
            st.html(
                f"""
                <div class="dd-kpi">
                    <div class="dd-kpi-label">{label}</div>
                    <div class="dd-kpi-value">{value}</div>
                    <div class="dd-kpi-meta">{meta}</div>
                </div>
                """,
            )

    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # Health + incident
    # ------------------------------------------------------------------

    st.markdown(
        '<div class="dd-section">Pipeline Intelligence</div>',
        unsafe_allow_html=True,
    )

    left, right = st.columns([1, 1])

    with left:
        completeness = 98 if not failed_runs else max(60, 98 - failed_runs * 8)
        freshness = 96 if not running_runs else 92
        reliability = success_rate

        health_rows = [
            ("Overall Health", health),
            ("Completeness", completeness),
            ("Freshness", freshness),
            ("Reliability", reliability),
        ]

        health_html = ""

        for label, value in health_rows:
            health_html += f"""
            <div class="health-line">
                <div class="health-label">
                    <span>{label}</span>
                    <span>{value}%</span>
                </div>
                <div class="health-track">
                    <div class="health-fill" style="width:{value}%"></div>
                </div>
            </div>
            """

        st.markdown(
            f"""
            <div class="dd-panel">
                <div class="dd-panel-title">Pipeline Health</div>
                {health_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    with right:
        if failed_runs:
            latest_failed = next(
                (r for r in runs if r.get("status") == "failed"),
                None,
            )

            run_id = (
                latest_failed.get("run_id", "unknown")
                if latest_failed
                else "unknown"
            )

            st.markdown("### Active Incident")

            with st.container(border=True):
                st.error("ðŸ”´ Pipeline failure detected")

                st.markdown(
                    f"**Run `{html.escape(str(run_id))}` requires diagnosis and repair.**"
                )

                st.caption(
                    "DataDoctor can inspect evidence, identify the failing "
                    "layer and generate a repair."
                )

        else:
            st.markdown("### Incident Monitor")

            with st.container(border=True):
                st.success("âœ“ All systems healthy")
                st.caption("No failed pipeline runs detected.")

    # ------------------------------------------------------------------
    # Recent runs
    # ------------------------------------------------------------------

    if runs:
        st.markdown(
            '<div class="dd-section">Recent Pipeline Activity</div>',
            unsafe_allow_html=True,
        )

        df_runs = pd.DataFrame(runs)

        available = [
            c
            for c in ["run_id", "dataset", "status", "started_at"]
            if c in df_runs.columns
        ]

        if available:
            df_view = df_runs[available].copy()

            if "started_at" in df_view.columns:
                try:
                    df_view["started_at"] = pd.to_datetime(
                        df_view["started_at"],
                        unit="s",
                        errors="coerce",
                    )
                except Exception:
                    pass

            st.dataframe(
                df_view,
                use_container_width=True,
                hide_index=True,
            )

    # ------------------------------------------------------------------
    # Gold datasets
    # ------------------------------------------------------------------

    if tables:
        st.markdown(
            '<div class="dd-section">Gold Layer</div>',
            unsafe_allow_html=True,
        )

        for table_name in tables:
            with st.expander(f"ðŸ“¦ {table_name}"):
                try:
                    gdf = db.read_table("gold", table_name)
                    st.dataframe(
                        gdf.head(20),
                        use_container_width=True,
                        hide_index=True,
                    )
                except Exception as exc:
                    st.warning(f"Could not load table: {exc}")
