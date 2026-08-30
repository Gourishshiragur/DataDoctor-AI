"""
dbx_enterprise/jobs.py
-----------------------
Wires the previously-inert `dbx_enterprise/notebooks/bronze_silver_gold_job.py` +
`jobs/job_config.json` into something the app can actually trigger, instead of being
a reference file you'd import into Databricks by hand.

Mode-aware, same as connection.py: pass `mode="demo"` to run against your free-edition
workspace, `mode="enterprise"` for a customer's paid workspace. Defaults to whichever
mode is currently active.

Flow (all via plain REST calls with `requests` â€” no databricks-sdk dependency needed):
1. `ensure_landing_volume`  -> CREATE VOLUME IF NOT EXISTS via the SQL Warehouse connection
                                already used for table reads/writes.
2. `upload_file_to_volume`  -> Files API PUT, lands the raw uploaded/demo CSV bytes at
                                /Volumes/{catalog}/{schema}/landing/{dataset_name}.csv
3. `deploy_notebook`        -> Workspace Import API, idempotently pushes the local
                                bronze_silver_gold_job.py notebook source into the
                                workspace (only needs to happen once per workspace).
4. `submit_job_run`         -> Jobs API one-time run (`/jobs/runs/submit`) pointing the
                                notebook at the uploaded file, passing the table names /
                                batch size / Delta housekeeping flags from
                                settings["pipeline"] as base_parameters. Defaults to
                                SERVERLESS compute (no `new_cluster` block) because
                                Databricks Free Edition workspaces generally don't
                                support provisioning classic all-purpose/job clusters â€”
                                only serverless SQL warehouses and serverless
                                notebook/job compute. Paid workspaces can pass
                                `cluster_spec` to use a classic cluster instead.
5. `get_run_status`         -> single bounded-timeout status check (never polls in a
                                loop server-side â€” the UI re-checks on a button click,
                                same lesson learned from the storage router hanging on
                                an un-cancellable connection attempt).

Every network call has an explicit `timeout=` â€” Databricks REST calls, unlike the SQL
connector, support this natively so no thread/executor trick is needed here. Transient
failures on the one-shot REST calls (upload/deploy/submit) get `pipeline.max_retries`
retries with `pipeline.retry_delay_seconds` backoff â€” deliberately NOT applied to
get_run_status or to the SQL Warehouse connection path, since those are either
polled by the user anyway or already covered by the router's own circuit breaker.
"""
from __future__ import annotations

import base64
import time
from pathlib import Path
from typing import Optional

import requests

from config.settings import current_mode, get_databricks_config, get_pipeline_settings, load_settings

REQUEST_TIMEOUT = 20  # seconds â€” bounded so a bad/slow workspace never hangs the UI

NOTEBOOK_SOURCE_PATH = Path(__file__).resolve().parent / "notebooks" / "bronze_silver_gold_job.py"
DEFAULT_WORKSPACE_NOTEBOOK_PATH = "/Shared/DataDoctorAI/bronze_silver_gold_job"


class DatabricksAPIError(RuntimeError):
    pass


def _cfg(mode: Optional[str] = None) -> dict:
    settings = load_settings()
    mode = mode or current_mode(settings)
    db = get_databricks_config(settings, mode)
    if not (db.get("workspace_url") and db.get("token")):
        raise DatabricksAPIError(f"Databricks workspace URL and token are required for {mode.title()} "
                                  f"Mode â€” check Settings.")
    return db


def _base_url(db_cfg: dict) -> str:
    return db_cfg["workspace_url"].rstrip("/")


def _headers(db_cfg: dict) -> dict:
    return {"Authorization": f"Bearer {db_cfg['token']}"}


def _raise_for_status(r: requests.Response):
    if not r.ok:
        try:
            detail = r.json().get("message", r.text)
        except Exception:  # noqa: BLE001
            detail = r.text
        raise DatabricksAPIError(f"HTTP {r.status_code}: {detail}")


def _with_retries(fn, *, max_retries: int, delay_seconds: float):
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_err = e
            if attempt < max_retries:
                time.sleep(delay_seconds)
    raise DatabricksAPIError(f"Failed after {max_retries + 1} attempt(s): {last_err}")


def ensure_landing_volume(catalog: Optional[str] = None, schema: Optional[str] = None,
                           mode: Optional[str] = None) -> str:
    """Creates the Unity Catalog Volume that uploaded files land in, if it doesn't exist."""
    from dbx_enterprise import connection

    db_cfg = _cfg(mode)
    catalog = catalog or db_cfg.get("catalog", "main")
    schema = schema or db_cfg.get("schema", "default")
    connection.execute_ddl(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`", mode=mode)
    connection.execute_ddl(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`landing`", mode=mode)
    return f"/Volumes/{catalog}/{schema}/landing"


def upload_file_to_volume(file_bytes: bytes, dataset_name: str, filename_suffix: str = "csv",
                           catalog: Optional[str] = None, schema: Optional[str] = None,
                           mode: Optional[str] = None) -> str:
    """Uploads raw file bytes to the landing Volume via the Files API. Returns the
    Volume path the notebook job will read from."""
    settings = load_settings()
    pipeline = get_pipeline_settings(settings)
    db_cfg = _cfg(mode)
    catalog = catalog or db_cfg.get("catalog", "main")
    schema = schema or db_cfg.get("schema", "default")
    volume_path = f"/Volumes/{catalog}/{schema}/landing/{dataset_name}.{filename_suffix}"
    url = f"{_base_url(db_cfg)}/api/2.0/fs/files{volume_path}"

    def _do():
        r = requests.put(url, headers=_headers(db_cfg), data=file_bytes, timeout=REQUEST_TIMEOUT)
        _raise_for_status(r)
        return r

    _with_retries(_do, max_retries=pipeline.get("max_retries", 2),
                  delay_seconds=pipeline.get("retry_delay_seconds", 3))
    return volume_path


def deploy_notebook(workspace_path: str = DEFAULT_WORKSPACE_NOTEBOOK_PATH,
                     mode: Optional[str] = None, content: Optional[str] = None) -> str:
    """Idempotently pushes a notebook into the workspace so a Job run can point at it.
    Pass `content` to deploy a user-edited notebook instead of the bundled auto-generated
    one â€” either way it's a real deploy via the Workspace Import API, not a template
    swap. Safe to call every time â€” OVERWRITE keeps the workspace copy in sync."""
    settings = load_settings()
    pipeline = get_pipeline_settings(settings)
    db_cfg = _cfg(mode)
    source = content if content is not None else NOTEBOOK_SOURCE_PATH.read_text(encoding="utf-8")
    payload = {
        "path": workspace_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "overwrite": True,
        "content": base64.b64encode(source.encode()).decode(),
    }
    url = f"{_base_url(db_cfg)}/api/2.0/workspace/import"

    def _do():
        r = requests.post(url, headers=_headers(db_cfg), json=payload, timeout=REQUEST_TIMEOUT)
        _raise_for_status(r)
        return r

    _with_retries(_do, max_retries=pipeline.get("max_retries", 2),
                  delay_seconds=pipeline.get("retry_delay_seconds", 3))
    return workspace_path


def get_bundled_notebook_source() -> str:
    """Returns the auto-generated notebook source as a starting point for editing."""
    return NOTEBOOK_SOURCE_PATH.read_text(encoding="utf-8")


def submit_job_run(source_path: str, dataset_name: str, notebook_path: str = DEFAULT_WORKSPACE_NOTEBOOK_PATH,
                    catalog: Optional[str] = None, schema: Optional[str] = None,
                    cluster_spec: Optional[dict] = None, mode: Optional[str] = None, datadoctor_run_id: Optional[str] = None) -> str:
    """Submits a one-time Databricks Job run. Defaults to serverless (no `new_cluster`
    block) since Free Edition workspaces generally can't provision classic clusters."""
    settings = load_settings()
    pipeline = get_pipeline_settings(settings)
    db_cfg = _cfg(mode)
    catalog = catalog or db_cfg.get("catalog", "main")
    schema = schema or db_cfg.get("schema", "default")

    task: dict = {
        "task_key": "bronze_silver_gold",
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": {
                "catalog": catalog,
                "schema": schema,
                "source_path": source_path,
                "dataset_name": dataset_name,
                "datadoctor_run_id": datadoctor_run_id or "",
                # Pulled straight from settings["pipeline"] â€” the notebook reads these
                # via dbutils.widgets instead of having them hardcoded.
                "bronze_table": pipeline.get("bronze_table", "bronze"),
                "silver_table": pipeline.get("silver_table", "silver"),
                "gold_table": pipeline.get("gold_table", "gold"),
                "batch_size": str(pipeline.get("batch_size", 50000)),
                "optimize_after_load": str(pipeline.get("optimize_after_load", True)),
                "vacuum_after_load": str(pipeline.get("vacuum_after_load", False)),
            },
        },
    }
    if cluster_spec:
        task["new_cluster"] = cluster_spec  # only for paid workspaces that want a classic cluster

    payload = {"run_name": f"DataDoctorAI - {dataset_name}", "tasks": [task]}
    url = f"{_base_url(db_cfg)}/api/2.0/jobs/runs/submit"

    def _do():
        r = requests.post(url, headers=_headers(db_cfg), json=payload, timeout=REQUEST_TIMEOUT)
        _raise_for_status(r)
        return r

    r = _with_retries(_do, max_retries=pipeline.get("max_retries", 2),
                       delay_seconds=pipeline.get("retry_delay_seconds", 3))
    return str(r.json()["run_id"])


def get_run_output(run_id: str, mode: Optional[str] = None) -> dict:
    """Fetch the actual output/error details for a Databricks job run."""
    db_cfg = _cfg(mode)
    url = f"{_base_url(db_cfg)}/api/2.0/jobs/runs/get-output"

    r = requests.get(
        url,
        headers=_headers(db_cfg),
        params={"run_id": run_id},
        timeout=REQUEST_TIMEOUT,
    )
    _raise_for_status(r)

    data = r.json()

    error = data.get("error", "")
    error_trace = data.get("error_trace", "")
    notebook_output = data.get("notebook_output") or {}

    return {
        "run_id": run_id,
        "error": error,
        "error_trace": error_trace,
        "notebook_output": notebook_output,
        "metadata": data.get("metadata") or {},
    }


def get_run_status(run_id: str, mode: Optional[str] = None) -> dict:
    """Return Databricks run state and task-level failure details.

    Performs one bounded-time API request. The UI controls refresh/polling.
    """
    db_cfg = _cfg(mode)
    url = f"{_base_url(db_cfg)}/api/2.0/jobs/runs/get"

    r = requests.get(
        url,
        headers=_headers(db_cfg),
        params={"run_id": run_id},
        timeout=REQUEST_TIMEOUT,
    )
    _raise_for_status(r)

    data = r.json()
    state = data.get("state", {})

    tasks = data.get("tasks") or []
    task_errors = []

    for task in tasks:
        task_key = task.get("task_key", "")
        task_state = task.get("state") or {}

        result_state = task_state.get("result_state", "")
        state_message = task_state.get("state_message", "")

        if result_state == "FAILED" or state_message:
            task_errors.append(
                {
                    "task_key": task_key,
                    "life_cycle_state": task_state.get(
                        "life_cycle_state",
                        "UNKNOWN",
                    ),
                    "result_state": result_state,
                    "state_message": state_message,
                }
            )

    error_message = ""

    if task_errors:
        parts = []

        for item in task_errors:
            label = item["task_key"] or "task"
            message = (
                item["state_message"]
                or item["result_state"]
                or "Unknown failure"
            )
            parts.append(f"{label}: {message}")

        error_message = " | ".join(parts)

    return {
        "run_id": run_id,
        "life_cycle_state": state.get(
            "life_cycle_state",
            "UNKNOWN",
        ),
        "result_state": state.get(
            "result_state",
            "",
        ),
        "state_message": state.get(
            "state_message",
            "",
        ),
        "error_message": error_message,
        "task_errors": task_errors,
        "run_page_url": data.get(
            "run_page_url",
            "",
        ),
        "start_time": data.get("start_time"),
        "end_time": data.get("end_time"),
    }
