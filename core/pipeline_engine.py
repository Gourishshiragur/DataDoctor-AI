"""
Executes a pipeline's steps in dependency order.

This is a *simulated* execution engine: it does not spin up a real Spark
cluster (a free-tier web app has no business trying to). Instead it models
step execution realistically enough to demonstrate the orchestration,
retry, and repair mechanics that are the actual point of this app —
timing, structured logging, and failure/retry state transitions behave the
same way they would against a real engine, just with an execution latency
and failure model swapped in for a real Databricks job run.

To point this at a real execution backend (Databricks Jobs API, for
example), swap `_execute_step()` for an API call and everything else
(dependency ordering, retry, logging, run state) is unchanged.
"""
import random
import time
from datetime import datetime, timezone

from core.models import Run, StepRun, new_id
from core import store

# steps whose code contains these substrings are more likely to fail in the
# simulation, to make "repair" demoable rather than purely cosmetic
RISKY_KEYWORDS = ["merge", "join", "cast", "regex"]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _log(run_id: str, step_id: str, level: str, message: str):
    store.append_log(
        {
            "run_id": run_id,
            "step_id": step_id,
            "level": level,
            "message": message,
            "timestamp": _now(),
        }
    )


def _topological_order(steps: list) -> list:
    by_id = {s["id"]: s for s in steps}
    visited, order = set(), []

    def visit(step_id, stack):
        if step_id in visited:
            return
        if step_id in stack:
            raise ValueError(f"circular dependency detected at step {step_id}")
        stack.add(step_id)
        for dep in by_id[step_id].get("depends_on", []):
            visit(dep, stack)
        stack.discard(step_id)
        visited.add(step_id)
        order.append(step_id)

    for s in steps:
        visit(s["id"], set())
    return [by_id[sid] for sid in order]


def _execute_step(step: dict, run_id: str) -> tuple:
    """Returns (succeeded: bool, error_message: str | None, rows: int | None)."""
    _log(run_id, step["id"], "INFO", f"starting step '{step['name']}' ({step['step_type']}, {step['engine']})")
    time.sleep(random.uniform(0.3, 0.9))  # simulated execution latency

    fail_chance = 0.12
    if any(k in step.get("code", "").lower() for k in RISKY_KEYWORDS):
        fail_chance = 0.35

    if random.random() < fail_chance:
        error = random.choice(
            [
                "AnalysisException: column 'device_id' not found in schema",
                "Delta MERGE conflict: concurrent write detected on target table",
                "OutOfMemoryError: Spark executor lost during shuffle",
                "TimeoutException: source read exceeded 300s",
            ]
        )
        _log(run_id, step["id"], "ERROR", f"step '{step['name']}' failed: {error}")
        return False, error, None

    rows = random.randint(500, 50000)
    _log(run_id, step["id"], "INFO", f"step '{step['name']}' succeeded, {rows} rows processed")
    return True, None, rows


def run_pipeline(pipeline: dict) -> dict:
    ordered_steps = _topological_order(pipeline["steps"])
    run = Run(id=new_id("run"), pipeline_id=pipeline["id"], pipeline_name=pipeline["name"])
    run.step_runs = [StepRun(step_id=s["id"]).__dict__ for s in ordered_steps]
    store.save_run(run.to_dict())
    _log(run.id, "-", "INFO", f"run started for pipeline '{pipeline['name']}'")

    failed_upstream = set()
    for step, step_run in zip(ordered_steps, run.step_runs):
        if any(dep in failed_upstream for dep in step.get("depends_on", [])):
            step_run["status"] = "SKIPPED"
            step_run["error_message"] = "upstream dependency failed"
            _log(run.id, step["id"], "WARN", f"skipping '{step['name']}': upstream dependency failed")
            failed_upstream.add(step["id"])
            continue

        step_run["status"] = "RUNNING"
        step_run["started_at"] = _now()
        store.save_run(run.to_dict())

        succeeded, error, rows = _execute_step(step, run.id)

        step_run["ended_at"] = _now()
        if succeeded:
            step_run["status"] = "SUCCEEDED"
            step_run["rows_processed"] = rows
        else:
            step_run["status"] = "FAILED"
            step_run["error_message"] = error
            failed_upstream.add(step["id"])

        store.save_run(run.to_dict())

    any_failed = any(sr["status"] in ("FAILED", "SKIPPED") for sr in run.step_runs)
    run.status = "FAILED" if any_failed else "SUCCEEDED"
    run.ended_at = _now()
    store.save_run(run.to_dict())
    _log(run.id, "-", "INFO", f"run finished with status {run.status}")
    return run.to_dict()
