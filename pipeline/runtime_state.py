"""
pipeline/runtime_state.py
-------------------------
Central runtime state for DataDoctorAI pipeline execution.

The UI, Monitor and Replay pages should consume this state instead of
inventing their own pipeline status or animation state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from threading import Lock
from typing import Any


STAGES = (
    "source",
    "bronze",
    "profiling",
    "quality",
    "repair",
    "silver",
    "gold",
)

STATES = (
    "waiting",
    "running",
    "success",
    "failed",
    "repairing",
    "retrying",
    "cancelled",
)

_lock = Lock()

_runs: dict[str, dict[str, Any]] = {}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_run(run_id: str, dataset: str, mode: str = "demo") -> dict[str, Any]:
    with _lock:
        state = {
            "run_id": run_id,
            "dataset": dataset,
            "mode": mode,
            "status": "running",
            "current_stage": "source",
            "started_at": _now(),
            "finished_at": None,
            "stages": {
                stage: {
                    "state": "waiting",
                    "started_at": None,
                    "finished_at": None,
                    "rows_in": 0,
                    "rows_out": 0,
                    "message": "",
                    "backend": None,
                }
                for stage in STAGES
            },
            "metrics": {
                "rows_in": 0,
                "rows_out": 0,
                "quality_score": None,
                "repairs": 0,
                "failed_checks": 0,
            },
            "events": [],
        }

        state["stages"]["source"]["state"] = "success"
        state["stages"]["source"]["started_at"] = state["started_at"]
        state["stages"]["source"]["finished_at"] = state["started_at"]

        _runs[run_id] = state
        return state.copy()


def update_stage(
    run_id: str,
    stage: str,
    state: str,
    *,
    rows_in: int | None = None,
    rows_out: int | None = None,
    message: str = "",
    backend: str | None = None,
) -> dict[str, Any]:
    if stage not in STAGES:
        raise ValueError(f"Unknown pipeline stage: {stage}")

    if state not in STATES:
        raise ValueError(f"Unknown pipeline state: {state}")

    with _lock:
        run = _runs.get(run_id)
        if not run:
            raise KeyError(f"Unknown run_id: {run_id}")

        stage_state = run["stages"][stage]

        stage_state["state"] = state
        stage_state["message"] = message

        if backend is not None:
            stage_state["backend"] = backend

        if rows_in is not None:
            stage_state["rows_in"] = int(rows_in)

        if rows_out is not None:
            stage_state["rows_out"] = int(rows_out)

        if state in {"running", "repairing", "retrying"}:
            stage_state["started_at"] = stage_state["started_at"] or _now()
            run["current_stage"] = stage
        elif state in {"success", "failed", "cancelled"}:
            stage_state["finished_at"] = _now()

            if state == "failed":
                run["status"] = "failed"
            elif state == "cancelled":
                run["status"] = "cancelled"

        run["events"].append({
            "timestamp": _now(),
            "stage": stage,
            "state": state,
            "message": message,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "backend": backend,
        })

        return run.copy()


def update_metrics(
    run_id: str,
    *,
    rows_in: int | None = None,
    rows_out: int | None = None,
    quality_score: float | None = None,
    repairs: int | None = None,
    failed_checks: int | None = None,
) -> dict[str, Any]:
    with _lock:
        run = _runs.get(run_id)
        if not run:
            raise KeyError(f"Unknown run_id: {run_id}")

        metrics = run["metrics"]

        if rows_in is not None:
            metrics["rows_in"] = int(rows_in)

        if rows_out is not None:
            metrics["rows_out"] = int(rows_out)

        if quality_score is not None:
            metrics["quality_score"] = float(quality_score)

        if repairs is not None:
            metrics["repairs"] = int(repairs)

        if failed_checks is not None:
            metrics["failed_checks"] = int(failed_checks)

        return run.copy()


def finish_run(run_id: str, status: str = "success", message: str = ""):
    if status not in {"success", "failed", "cancelled"}:
        raise ValueError(f"Invalid final status: {status}")

    with _lock:
        run = _runs.get(run_id)
        if not run:
            raise KeyError(f"Unknown run_id: {run_id}")

        run["status"] = status
        run["finished_at"] = _now()

        if status == "success":
            run["current_stage"] = "gold"
            run["stages"]["gold"]["state"] = "success"
            run["stages"]["gold"]["finished_at"] = run["finished_at"]

        run["events"].append({
            "timestamp": run["finished_at"],
            "stage": run["current_stage"],
            "state": status,
            "message": message,
        })

        return run.copy()


def get_run(run_id: str) -> dict[str, Any] | None:
    with _lock:
        run = _runs.get(run_id)
        return run.copy() if run else None


def get_all_runs() -> list[dict[str, Any]]:
    with _lock:
        return [run.copy() for run in _runs.values()]


def clear_runtime_state() -> None:
    with _lock:
        _runs.clear()


def recover_run(run_id: str) -> dict[str, Any] | None:
    """Reconstruct runtime state from durable SQLite history.

    SQLite/history is authoritative for terminal state. A recovered terminal
    run must never briefly or permanently appear as RUNNING in the UI.
    """
    from database import history

    stored = history.get_run(run_id)
    if not stored:
        return None

    stored_status = str(stored.get("status") or "running").lower()

    # Build the in-memory structure first so historical events can be replayed.
    state = create_run(
        run_id,
        stored["dataset"],
        mode="demo",
    )

    events = history.get_events(run_id)

    for event in events:
        stage = event.get("stage")
        message = event.get("message", "")

        if stage not in STAGES:
            continue

        text = str(message).lower()

        if any(word in text for word in ("failed", "error", "exception")):
            stage_state = "failed"
        elif any(
            word in text
            for word in ("complete", "completed", "success", "materialized", "wrote")
        ):
            stage_state = "success"
        elif any(word in text for word in ("repair", "self-healing")):
            stage_state = "repairing"
        else:
            stage_state = "running"

        update_stage(
            run_id,
            stage,
            stage_state,
            message=message,
        )

    # CRITICAL:
    # The durable database status is authoritative.
    if stored_status in {"success", "failed", "cancelled"}:
        with _lock:
            run = _runs.get(run_id)

            if run:
                run["status"] = stored_status
                run["finished_at"] = (
                    datetime.fromtimestamp(
                        stored["finished_at"],
                        tz=timezone.utc,
                    ).isoformat()
                    if stored.get("finished_at")
                    else None
                )

                if stored_status == "failed":
                    run["current_stage"] = (
                        run.get("current_stage")
                        if run.get("current_stage") in STAGES
                        else "bronze"
                    )

                elif stored_status == "cancelled":
                    run["current_stage"] = (
                        run.get("current_stage")
                        if run.get("current_stage") in STAGES
                        else "bronze"
                    )

    return get_run(run_id)

