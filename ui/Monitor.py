"""DataDoctorAI Monitor ? operational health, alerts and real self-healing repair."""
from __future__ import annotations

import json
import time

import pandas as pd
import plotly.express as px
import streamlit as st

from ai.repair_engine import repair
from config.settings import get_monitoring_settings, get_quality_settings, load_settings
from database import history
from monitoring import alerts
from pipeline import quality, runtime_state
from storage import router as db


def _summary(run):
    raw = run.get("summary") or {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return raw if isinstance(raw, dict) else {}


def _repair_run(run):
    run_id = run["run_id"]
    dataset = run["dataset"]
    settings = load_settings()
    quality_settings = get_quality_settings(settings)

    source_layer = None
    source_df = None

    for layer in ("bronze", "silver"):
        try:
            if db.table_exists(layer, dataset, run_id=run_id):
                source_df = db.read_table(layer, dataset, run_id=run_id)
                source_layer = layer
                break
        except Exception:
            pass

    if source_df is None:
        for layer in ("bronze", "silver"):
            try:
                if db.table_exists(layer, dataset):
                    source_df = db.read_table(layer, dataset)
                    source_layer = layer
                    break
            except Exception:
                pass

    if source_df is None or source_df.empty:
        raise RuntimeError(
            f"No materialized {dataset} data was found in Bronze/Silver. "
            "The failed run cannot be repaired because there is no real input data."
        )

    working_df = source_df.drop(
        columns=["_ingested_at", "_row_hash"],
        errors="ignore",
    ).copy()

    history.log_event(
        run_id,
        "repair",
        f"Repair started from {source_layer} data: {len(working_df):,} rows",
    )

    runtime_state.update_stage(
        run_id,
        "repair",
        "running",
        rows_in=len(working_df),
        message=f"Diagnosing and repairing {dataset} from {source_layer}",
    )

    pre_checks = quality.run_all_checks(working_df, quality_settings)

    for check in pre_checks:
        history.log_quality_check(
            run_id,
            check["check_name"],
            "repair_precheck",
            check["passed"],
            check["details"],
        )

    repaired_df, actions = repair(working_df, explain=True)

    for action in actions:
        history.log_repair(
            run_id,
            action["column_name"],
            action["issue"],
            action["action"],
            action["rows_affected"],
            action["source"],
        )

    post_checks = quality.run_all_checks(repaired_df, quality_settings)

    for check in post_checks:
        history.log_quality_check(
            run_id,
            check["check_name"],
            "silver",
            check["passed"],
            check["details"],
        )

    score_before = quality.quality_score(pre_checks)
    score_after = quality.quality_score(post_checks)

    db.write_table("silver", dataset, repaired_df, run_id=run_id)

    history.log_event(
        run_id,
        "repair",
        f"Repair completed: {len(actions)} action(s), quality {score_before}/100 -> {score_after}/100",
    )

    runtime_state.update_stage(
        run_id,
        "repair",
        "success",
        rows_in=len(working_df),
        rows_out=len(repaired_df),
        message=f"{len(actions)} repair action(s); quality {score_before} -> {score_after}",
    )

    minimum_score = quality_settings.get("minimum_score", 70)

    if score_after >= minimum_score:
        runtime_state.update_stage(
            run_id,
            "silver",
            "success",
            rows_out=len(repaired_df),
            message=f"Verified repaired Silver dataset - quality {score_after}/100",
        )

        history.finish_run(
            run_id,
            "success",
            {
                **_summary(run),
                "resolution": "manual_self_heal",
                "source_layer": source_layer,
                "repair_actions": len(actions),
                "quality_before": score_before,
                "quality_after": score_after,
                "verified": True,
            },
        )
    else:
        history.finish_run(
            run_id,
            "failed",
            {
                **_summary(run),
                "resolution": "repair_attempted_but_quality_below_threshold",
                "source_layer": source_layer,
                "repair_actions": len(actions),
                "quality_before": score_before,
                "quality_after": score_after,
                "verified": False,
            },
        )

    return {
        "source_layer": source_layer,
        "rows_before": len(working_df),
        "rows_after": len(repaired_df),
        "actions": actions,
        "pre_checks": pre_checks,
        "post_checks": post_checks,
        "score_before": score_before,
        "score_after": score_after,
    }


def render():
    st.title("Monitor")

    history_days = get_monitoring_settings(load_settings()).get("history_days", 30)
    cutoff = time.time() - history_days * 86400

    st.caption(
        f"Showing the last {history_days} days (change under Settings -> Monitoring)."
    )

    health = alerts.health_summary()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Runs", health["total_runs"])
    c2.metric("Succeeded", health["succeeded"])
    c3.metric("Failed", health["failed"])
    c4.metric("Success Rate", f"{health['success_rate']}%")

    st.divider()
    st.subheader("Active Alerts")

    active_alerts = alerts.get_active_alerts()

    if not active_alerts:
        st.success(
            "No active alerts - all recent runs passed quality checks after self-healing."
        )
    else:
        for a in active_alerts:
            icon = "" if a["severity"] == "error" else ""
            st.markdown(
                f"{icon} **[{a['dataset']}  - run {a['run_id']}]** {a['message']}"
            )

            if a["details"]:
                with st.expander("Details"):
                    st.json(a["details"])

    st.divider()
    st.subheader("Diagnose & Repair")

    st.caption(
        "Select a failed run. DataDoctor reads real materialized data, "
        "runs quality checks, executes the existing self-healing engine, "
        "writes Silver and verifies the result."
    )

    runs = [
        r for r in history.get_runs(limit=100)
        if (r.get("started_at") or 0) >= cutoff
    ]

    failed_runs = []
    for r in runs:
        if r.get("status") != "failed":
            continue

        summary = _summary(r)
        reason = str(summary.get("reason", "")).lower()

        # These are historical/system-created failures, not repairable
        # pipeline failures because no real pipeline execution/data exists.
        if reason.startswith("orphaned_"):
            continue

        failed_runs.append(r)

    if failed_runs:
        options = {
            f"{r['run_id']} - {r['dataset']} - FAILED": r
            for r in failed_runs
        }

        selected_label = st.selectbox(
            "Failed pipeline run",
            list(options.keys()),
            key="monitor_repair_run",
        )

        selected_run = options[selected_label]
        selected_summary = _summary(selected_run)

        m1, m2, m3 = st.columns(3)
        m1.metric("Run", selected_run["run_id"])
        m2.metric("Dataset", selected_run["dataset"])
        m3.metric("Engine", selected_summary.get("engine", "Recorded run"))

        if selected_summary:
            with st.expander("Run diagnosis evidence", expanded=True):
                st.json(selected_summary)

        if st.button(
            "Diagnose & Repair Now",
            type="primary",
            use_container_width=True,
            key=f"repair_{selected_run['run_id']}",
        ):
            try:
                with st.status(
                    "DataDoctor diagnosing and repairing...",
                    expanded=True,
                ) as status:

                    st.write(
                        f"Reading real `{selected_run['dataset']}` data..."
                    )

                    result = _repair_run(selected_run)

                    st.write(
                        f"Detected {len(result['actions'])} repair action(s). "
                        f"Quality: {result['score_before']}/100 -> "
                        f"{result['score_after']}/100."
                    )

                    status.update(
                        label="? Repair and verification complete",
                        state="complete",
                    )

                if result["actions"]:
                    st.success(
                        f"Self-healing completed: {len(result['actions'])} "
                        "repair action(s) were applied and Silver was materialized."
                    )

                    st.dataframe(
                        pd.DataFrame(result["actions"]),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.info(
                        "No deterministic repair action was required. "
                        f"Verified quality score: {result['score_after']}/100."
                    )

                st.rerun()

            except Exception as exc:
                st.error(f"Repair could not be completed: {exc}")

    else:
        st.info("No failed runs available for repair.")

    st.divider()
    st.subheader("Repair History (all runs)")

    repairs = history.get_repairs()

    if repairs:
        rep_df = pd.DataFrame(repairs)

        st.dataframe(
            rep_df,
            use_container_width=True,
            hide_index=True,
        )

        if "issue" in rep_df.columns:
            counts = (
                rep_df["issue"]
                .astype(str)
                .str.extract(r"([a-zA-Z ]+)")
                .fillna("other")
            )

            counts.columns = ["issue_type"]
            counts["count"] = 1

            agg = (
                counts.groupby("issue_type")["count"]
                .sum()
                .reset_index()
                .sort_values("count", ascending=False)
                .head(10)
            )

            fig = px.bar(
                agg,
                x="issue_type",
                y="count",
                title="Most common issues auto-repaired",
            )

            fig.update_layout(
                height=320,
                margin=dict(l=10, r=10, t=40, b=10),
            )

            st.plotly_chart(fig, use_container_width=True)

    else:
        st.caption("No repairs logged yet.")

    st.divider()
    st.subheader("Storage Backend History")

    backend_events = [
        e for e in history.get_backend_events(limit=200)
        if e["ts"] >= cutoff
    ]

    if backend_events:
        bdf = pd.DataFrame(backend_events)

        fallback_count = int(bdf["fallback"].sum())
        total = len(bdf)

        if fallback_count:
            st.warning(
                f"{fallback_count}/{total} recent storage operations fell back "
                "to DuckDB (Databricks was requested but unavailable)."
            )
        else:
            st.caption(f"{total} recent storage operations logged.")

        bdf_display = bdf[
            [
                "ts",
                "layer",
                "table_name",
                "requested_backend",
                "actual_backend",
                "fallback",
                "reason",
            ]
        ].copy()

        bdf_display["ts"] = (
            pd.to_datetime(bdf_display["ts"], unit="s")
            .dt.strftime("%H:%M:%S")
        )

        st.dataframe(
            bdf_display,
            use_container_width=True,
            hide_index=True,
        )

    else:
        st.caption(
            "No storage operations logged yet - run a pipeline in Pipeline Studio."
        )

    st.divider()
    st.subheader("Run Timeline")

    if runs:
        run_ids = [r["run_id"] for r in runs]

        selected = st.selectbox(
            "Select a run to inspect",
            run_ids,
            key="monitor_timeline_run",
        )

        events = history.get_events(selected)

        if events:
            for e in events:
                ts = (
                    pd.to_datetime(e["ts"], unit="s")
                    .strftime("%H:%M:%S")
                )

                st.write(
                    f"`{ts}` **[{e['stage']}]** {e['message']}"
                )
        else:
            st.caption(
                "No timeline events were logged for this run."
            )
    else:
        st.caption("No runs yet.")

