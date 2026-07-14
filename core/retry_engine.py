"""
Retry/repair logic for a failed run.

Two modes, matching real operational practice:
  - retry_step(): re-runs a single failed step in place (for a step that
    failed due to a transient issue — timeout, executor loss)
  - repair_run(): re-runs every FAILED/SKIPPED step in a run, in dependency
    order, leaving already-SUCCEEDED steps untouched (mirrors "repair run"
    functionality in Databricks Jobs / ADF pipeline re-run-from-failure)
"""
from datetime import datetime, timezone

from core import store
from core.pipeline_engine import _execute_step, _log, _topological_order

MAX_RETRIES_PER_STEP = 3


def _now():
    return datetime.now(timezone.utc).isoformat()


def retry_step(run: dict, pipeline: dict, step_id: str) -> dict:
    step = next(s for s in pipeline["steps"] if s["id"] == step_id)
    step_run = next(sr for sr in run["step_runs"] if sr["step_id"] == step_id)

    if step_run["retry_count"] >= MAX_RETRIES_PER_STEP:
        _log(run["id"], step_id, "ERROR", f"max retries ({MAX_RETRIES_PER_STEP}) exceeded for '{step['name']}'")
        return run

    step_run["retry_count"] += 1
    step_run["status"] = "RUNNING"
    step_run["started_at"] = _now()
    store.save_run(run)

    succeeded, error, rows = _execute_step(step, run["id"])

    step_run["ended_at"] = _now()
    step_run["status"] = "SUCCEEDED" if succeeded else "FAILED"
    step_run["error_message"] = None if succeeded else error
    step_run["rows_processed"] = rows if succeeded else step_run.get("rows_processed")

    run["status"] = _recompute_run_status(run)
    store.save_run(run)
    return run


def repair_run(run: dict, pipeline: dict) -> dict:
    ordered_steps = _topological_order(pipeline["steps"])
    step_runs_by_id = {sr["step_id"]: sr for sr in run["step_runs"]}

    for step in ordered_steps:
        step_run = step_runs_by_id[step["id"]]
        if step_run["status"] not in ("FAILED", "SKIPPED"):
            continue

        upstream_ok = all(
            step_runs_by_id[dep]["status"] == "SUCCEEDED" for dep in step.get("depends_on", [])
        )
        if not upstream_ok:
            step_run["status"] = "SKIPPED"
            step_run["error_message"] = "upstream still failing"
            continue

        if step_run["retry_count"] >= MAX_RETRIES_PER_STEP:
            _log(run["id"], step["id"], "WARN", f"'{step['name']}' left FAILED — max retries reached")
            continue

        step_run["retry_count"] += 1
        step_run["status"] = "RUNNING"
        step_run["started_at"] = _now()
        store.save_run(run)

        succeeded, error, rows = _execute_step(step, run["id"])
        step_run["ended_at"] = _now()
        step_run["status"] = "SUCCEEDED" if succeeded else "FAILED"
        step_run["error_message"] = None if succeeded else error
        if succeeded:
            step_run["rows_processed"] = rows

    run["status"] = _recompute_run_status(run)
    run["ended_at"] = _now()
    store.save_run(run)
    _log(run["id"], "-", "INFO", f"repair completed, run status now {run['status']}")
    return run


def _recompute_run_status(run: dict) -> str:
    statuses = [sr["status"] for sr in run["step_runs"]]
    if all(s == "SUCCEEDED" for s in statuses):
        return "SUCCEEDED"
    if any(s in ("FAILED", "SKIPPED") for s in statuses):
        was_all_failed_before = run["status"] == "FAILED"
        any_succeeded_now = any(s == "SUCCEEDED" for s in statuses)
        return "PARTIALLY_REPAIRED" if (was_all_failed_before and any_succeeded_now) else "FAILED"
    return "RUNNING"
