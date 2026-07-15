"""Optional Databricks Free Edition execution backend for DataDoctor AI.

Uses REST APIs only (requests is already a dependency). It uploads the source
file to a Unity Catalog Volume, imports a generated PySpark notebook, submits a
serverless notebook run, waits for completion, and returns auditable metadata.
No credential is stored in source code.
"""
from __future__ import annotations
import base64, os, re, time, uuid
from pathlib import Path
from typing import Any, Dict
import requests


def _cfg(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    try:
        import streamlit as st
        value = st.secrets.get(name, value)
    except Exception:
        pass
    return str(value).strip()


def configured() -> bool:
    return bool(_cfg("DATABRICKS_HOST") and _cfg("DATABRICKS_TOKEN"))


def _headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {_cfg('DATABRICKS_TOKEN')}"}


def _safe_name(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_]", "_", value).strip("_").lower()
    return value[:80] or "uploaded_data"


def _request(method: str, path: str, **kwargs):
    host = _cfg("DATABRICKS_HOST").rstrip("/")
    response = requests.request(method, host + path, headers=_headers(), timeout=120, **kwargs)
    response.raise_for_status()
    return response


def execute_file_pipeline(pipeline: Dict[str, Any]) -> Dict[str, Any]:
    """Execute uploaded data through real Databricks PySpark when configured."""
    if not configured():
        return {"attempted": False, "success": False, "reason": "Databricks is not configured."}
    source_path = pipeline.get("source_local_path")
    if not source_path or not Path(source_path).exists():
        return {"attempted": False, "success": False, "reason": "Uploaded source file is not available locally."}

    catalog = _cfg("DATABRICKS_CATALOG", "workspace")
    schema = _cfg("DATABRICKS_SCHEMA", "datadoctor")
    volume = _cfg("DATABRICKS_VOLUME", "source_files")
    run_key = uuid.uuid4().hex[:10]
    file_name = Path(source_path).name
    table = _safe_name(Path(file_name).stem)
    volume_path = f"/Volumes/{catalog}/{schema}/{volume}/{run_key}_{file_name}"
    notebook_path = f"/Users/{_cfg('DATABRICKS_USER', 'datadoctor')}/datadoctor_runs/{run_key}"

    # Upload source to a UC Volume.
    with open(source_path, "rb") as fh:
        _request("PUT", f"/api/2.0/fs/files{volume_path}", data=fh, headers={**_headers(), "Content-Type": "application/octet-stream"})

    code = f'''# Databricks notebook source
from pyspark.sql import functions as F
source_path = {volume_path!r}
catalog = {catalog!r}
schema = {schema!r}
table = {table!r}
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{catalog}}.{{schema}}")
ext = source_path.rsplit('.', 1)[-1].lower()
if ext == 'csv':
    df = spark.read.option('header', True).option('inferSchema', True).csv(source_path)
elif ext == 'json':
    df = spark.read.json(source_path)
elif ext == 'parquet':
    df = spark.read.parquet(source_path)
else:
    raise ValueError('Databricks execution currently supports CSV, JSON, and Parquet. Excel remains available in local verified mode.')
bronze = f"{{catalog}}.{{schema}}.bronze_{{table}}"
silver = f"{{catalog}}.{{schema}}.silver_{{table}}"
gold = f"{{catalog}}.{{schema}}.gold_{{table}}"
df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(bronze)
silver_df = df.dropDuplicates()
silver_df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(silver)
gold_df = silver_df.agg(F.count('*').alias('processed_rows'))
gold_df.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable(gold)
print({{'bronze_table': bronze, 'silver_table': silver, 'gold_table': gold, 'input_rows': df.count(), 'silver_rows': silver_df.count()}})
'''
    encoded = base64.b64encode(code.encode()).decode()
    _request("POST", "/api/2.0/workspace/import", json={"path": notebook_path, "format": "SOURCE", "language": "PYTHON", "content": encoded, "overwrite": True})
    payload = {"run_name": f"DataDoctor-{run_key}", "tasks": [{"task_key": "bronze_silver_gold", "notebook_task": {"notebook_path": notebook_path}, "environment_key": "default"}], "environments": [{"environment_key": "default", "spec": {"client": "1"}}]}
    submitted = _request("POST", "/api/2.1/jobs/runs/submit", json=payload).json()
    run_id = submitted["run_id"]
    deadline = time.time() + int(_cfg("DATABRICKS_RUN_TIMEOUT_SECONDS", "900"))
    state = {}
    while time.time() < deadline:
        details = _request("GET", "/api/2.1/jobs/runs/get", params={"run_id": run_id}).json()
        state = details.get("state", {})
        if state.get("life_cycle_state") in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            break
        time.sleep(5)
    success = state.get("result_state") == "SUCCESS"
    return {"attempted": True, "success": success, "backend": "databricks-serverless-pyspark", "databricks_run_id": run_id, "state": state, "source_volume_path": volume_path, "catalog": catalog, "schema": schema, "tables": {"bronze": f"{catalog}.{schema}.bronze_{table}", "silver": f"{catalog}.{schema}.silver_{table}", "gold": f"{catalog}.{schema}.gold_{table}"}, "reason": state.get("state_message", "")}
