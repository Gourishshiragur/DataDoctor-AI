"""
storage/router.py
------------------
This is the piece that actually makes Demo/Enterprise mode real instead of cosmetic.

Every pipeline call (write_table/read_table/table_exists/list_tables/run_sql) comes
through here instead of hitting `storage.db` or `dbx_enterprise.connection` directly.

Behavior — BOTH modes try Databricks first, then fall back to DuckDB:
- Demo Mode: tries the Demo-workspace credentials (meant to be your own free-edition
  Databricks workspace). If not configured, or any connection attempt fails/times out
  (cluster asleep, quota, network) -> falls back to local DuckDB.
- Enterprise Mode: same thing, but against the Enterprise-workspace credentials (a
  customer's paid/production workspace).
The two credential slots are independent (settings["databricks"]["demo"] vs
["enterprise"]) so switching the mode toggle never mixes them up.

Every call is logged to database.history.backend_events (when a run_id is supplied)
so Monitor/Dashboard can show, per run, which backend actually served each layer —
not just which mode was selected in Settings.
"""
from __future__ import annotations

import concurrent.futures
import time
from typing import List, Optional

import pandas as pd

from config.settings import current_mode, is_databricks_configured, load_settings
from storage import db as duckdb_backend

# databricks-sql-connector has no built-in connect timeout, so an unreachable
# free-edition cluster/warehouse (asleep, quota-exceeded, network-blocked) can hang
# indefinitely instead of failing over. Bound every Databricks attempt with a hard
# wall-clock timeout so the app always degrades to DuckDB within a predictable window.
DATABRICKS_TIMEOUT_SECONDS = 8
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="dbx-attempt")

# Circuit breaker: without this, a single unreachable workspace makes EVERY layer of
# EVERY run (bronze, silver, gold, each dashboard read) independently wait out the
# full timeout before falling back — a 3-layer pipeline run would take 3x as long as
# it should. After one failure, skip straight to DuckDB for a cooldown window instead
# of re-attempting a connection we already know is failing. Tracked per-mode so a dead
# Demo workspace doesn't also silence a healthy Enterprise workspace or vice versa.
CIRCUIT_COOLDOWN_SECONDS = 90
_circuit_open_until = {"demo": 0.0, "enterprise": 0.0}
_circuit_last_reason = {"demo": "", "enterprise": ""}

# Last-known status, kept in-process so pages rendered in the same server session can
# show a live badge without re-hitting the database. database.history is the durable
# record that survives restarts and is what Monitor reads for history across runs.
_last_status = {
    "mode": "demo",
    "requested_backend": "duckdb",
    "actual_backend": "duckdb",
    "fallback": False,
    "reason": "",
}


def get_last_status() -> dict:
    return dict(_last_status)


def describe_configuration(mode: Optional[str] = None) -> dict:
    """What backend WOULD be used, without attempting any connection — for UI badges
    shown before a pipeline has actually run (avoids the earlier bug where the sidebar
    claimed 'Using Databricks' just because that was the default setting, regardless of
    whether anything was configured or had ever successfully connected)."""
    settings = load_settings()
    mode = mode or current_mode(settings)
    configured = is_databricks_configured(settings, mode)
    return {
        "mode": mode,
        "databricks_configured": configured,
        "will_attempt": "databricks (falls back to duckdb if unreachable)" if configured else "duckdb only",
    }


def _log(mode: str, run_id: Optional[str], layer: str, table_name: str, requested: str,
         actual: str, fallback: bool, reason: str):
    _last_status.update({
        "mode": mode,
        "requested_backend": requested,
        "actual_backend": actual,
        "fallback": fallback,
        "reason": reason,
    })
    if run_id:
        # Imported lazily to avoid a circular import at module load time.
        from database import history
        history.log_backend_event(run_id, layer, table_name, requested, actual, fallback, reason)


def _databricks_backend():
    # NOTE: this app's own Databricks integration package is named `dbx_enterprise`,
    # deliberately NOT `databricks` — a local package named `databricks` would shadow
    # the real `databricks-sql-connector` PyPI package of the same name whenever the
    # app runs from its own root (which Streamlit always does), permanently breaking
    # the real connection import regardless of whether the driver is installed.
    from dbx_enterprise import connection as db_backend
    return db_backend


def _dispatch(op_name: str, layer: str, table_name: str, run_id: Optional[str], *args, **kwargs):
    """Runs `op_name` (write_table/read_table/table_exists/list_tables/run_sql) against
    the current mode's Databricks workspace if configured, catching any failure and
    falling back to DuckDB."""
    settings = load_settings()
    mode = current_mode(settings)
    configured = is_databricks_configured(settings, mode)

    if configured:
        now = time.time()
        if now < _circuit_open_until[mode]:
            remaining = round(_circuit_open_until[mode] - now)
            reason = (f"skipped — {mode} Databricks marked unavailable, cooling down for "
                      f"{remaining}s more (last failure: {_circuit_last_reason[mode]})")
            result = getattr(duckdb_backend, op_name)(*args, **kwargs)
            _log(mode, run_id, layer, table_name, "databricks", "duckdb", True, reason)
            return result
        try:
            backend = _databricks_backend()
            future = _executor.submit(getattr(backend, op_name), *args, mode=mode, **kwargs)
            result = future.result(timeout=DATABRICKS_TIMEOUT_SECONDS)
            _log(mode, run_id, layer, table_name, "databricks", "databricks", False, "")
            return result
        except concurrent.futures.TimeoutError:
            # The connection attempt is left running in the background (the driver
            # gives no way to cancel it) but we don't block the user's pipeline on it.
            reason = (f"TimeoutError: no response within {DATABRICKS_TIMEOUT_SECONDS}s "
                      f"({mode} cluster/warehouse likely asleep or unreachable)")
            _circuit_open_until[mode] = time.time() + CIRCUIT_COOLDOWN_SECONDS
            _circuit_last_reason[mode] = reason
            result = getattr(duckdb_backend, op_name)(*args, **kwargs)
            _log(mode, run_id, layer, table_name, "databricks", "duckdb", True, reason)
            return result
        except Exception as e:  # noqa: BLE001 — deliberately broad: any Databricks
            # failure (missing driver, free-edition cluster asleep/unavailable, auth,
            # network, quota) should degrade gracefully rather than crash the run.
            reason = f"{type(e).__name__}: {e}"
            _circuit_open_until[mode] = time.time() + CIRCUIT_COOLDOWN_SECONDS
            _circuit_last_reason[mode] = reason
            result = getattr(duckdb_backend, op_name)(*args, **kwargs)
            _log(mode, run_id, layer, table_name, "databricks", "duckdb", True, reason)
            return result
    else:
        result = getattr(duckdb_backend, op_name)(*args, **kwargs)
        reason = f"Databricks not configured for {mode.title()} Mode — check Settings."
        _log(mode, run_id, layer, table_name, "duckdb", "duckdb", False, reason)
        return result


def write_table(layer: str, name: str, df: pd.DataFrame, run_id: Optional[str] = None) -> str:
    return _dispatch("write_table", layer, name, run_id, layer, name, df)


def read_table(layer: str, name: str, run_id: Optional[str] = None) -> pd.DataFrame:
    return _dispatch("read_table", layer, name, run_id, layer, name)


def table_exists(layer: str, name: str, run_id: Optional[str] = None) -> bool:
    return _dispatch("table_exists", layer, name, run_id, layer, name)


def list_tables(layer: str | None = None, run_id: Optional[str] = None) -> List[str]:
    return _dispatch("list_tables", layer or "*", "*", run_id, layer)


def run_sql(query: str, run_id: Optional[str] = None) -> pd.DataFrame:
    return _dispatch("run_sql", "*", "*", run_id, query)
