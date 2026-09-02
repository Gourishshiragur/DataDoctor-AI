"""
dbx_enterprise/connection.py
------------------------------
Real Databricks SQL Warehouse connector, implementing the SAME interface as
storage/db.py so pipeline modules can call either transparently:

    write_table(layer, name, df)
    read_table(layer, name)
    table_exists(layer, name)
    list_tables(layer)

Mode-aware: every function accepts an optional `mode` ("demo" or "enterprise"). Demo
Mode is meant to point at your own Databricks Free Edition workspace; Enterprise Mode
at a customer's paid/production workspace. They're separate credential slots in
Settings (settings["databricks"]["demo"] vs settings["databricks"]["enterprise"]) so
switching modes never mixes the two up. If `mode` isn't passed, the currently active
mode is used.

Requires `databricks-sql-connector` (see requirements.txt — commented out by default
since Demo Mode doesn't strictly need it if you're using local DuckDB). Import is done
lazily so the whole app still runs fine without the package installed.
"""
from __future__ import annotations

from typing import List, Optional

import pandas as pd

from config.settings import current_mode, get_databricks_config, load_settings


def _get_connection(mode=None):
    print(">>> INSIDE _get_connection <<<")
    import importlib.metadata

    print(
    "Connector:",
    importlib.metadata.version("databricks-sql-connector")
)
    from databricks import sql

    settings = load_settings()
    mode = mode or current_mode(settings)
    cfg = get_databricks_config(settings, mode)

    print("=" * 80)
    print(cfg)
    print("=" * 80)

    raw_host = str(cfg.get("workspace_url", "")).strip()

    # Defensive normalization for Streamlit secrets / pasted values.
    # Accept https://host, host, and accidental leading "$".
    raw_host = raw_host.lstrip("$").strip()
    if raw_host.startswith("https://"):
        server_hostname = raw_host[len("https://"):]
    elif raw_host.startswith("http://"):
        server_hostname = raw_host[len("http://"):]
    else:
        server_hostname = raw_host

    server_hostname = server_hostname.rstrip("/").strip()

    if not server_hostname:
        raise ValueError("Databricks workspace host is empty.")

    if not cfg.get("token"):
        raise ValueError("Databricks access token is empty.")

    if not cfg.get("http_path"):
        raise ValueError("Databricks HTTP path is empty.")

    print("Databricks server hostname configured:", bool(server_hostname))
    print("Databricks HTTP path configured:", bool(cfg.get("http_path")))
    print("Databricks token configured:", bool(cfg.get("token")))

    conn = sql.connect(
        server_hostname=server_hostname,
        http_path=cfg["http_path"],
        access_token=cfg["token"],
    )

    print("CONNECTED SUCCESSFULLY")

    return conn

def _qualified(layer: str, name: str, mode: Optional[str] = None) -> str:
    settings = load_settings()
    mode = mode or current_mode(settings)
    cfg = get_databricks_config(settings, mode)
    catalog, schema = cfg.get("catalog", "main"), cfg.get("schema", "default")
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    return f"{catalog}.{schema}.{layer}__{safe_name}"


def write_table(layer: str, name: str, df: pd.DataFrame, mode: Optional[str] = None) -> str:
    """Writes via a staging approach: creates table from an inline VALUES statement.
    For large data, prefer the native Databricks Job path (see dbx_enterprise/jobs.py)
    which uses a proper Spark DataFrame writer instead."""
    table = _qualified(layer, name, mode)
    conn = _get_connection(mode)
    try:
        cur = conn.cursor()
        cols_def = ", ".join(f"`{c}` STRING" for c in df.columns)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols_def}) USING DELTA")
        cur.execute(f"TRUNCATE TABLE {table}")
        if len(df) > 0:
            values = ",".join(
                "(" + ",".join("NULL" if pd.isna(v) else "'" + str(v).replace("'", "''") + "'" for v in row) + ")"
                for row in df.itertuples(index=False)
            )
            cur.execute(f"INSERT INTO {table} VALUES {values}")
        cur.close()
    finally:
        conn.close()
    return table


def read_table(layer: str, name: str, mode: Optional[str] = None) -> pd.DataFrame:
    table = _qualified(layer, name, mode)
    conn = _get_connection(mode)
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {table}")
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def table_exists(layer: str, name: str, mode: Optional[str] = None) -> bool:
    table = _qualified(layer, name, mode)
    conn = _get_connection(mode)
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"DESCRIBE TABLE {table}")
            cur.fetchall()
            return True
        except Exception:
            return False
        finally:
            cur.close()
    finally:
        conn.close()


def list_tables(layer: Optional[str] = None, mode: Optional[str] = None) -> List[str]:
    settings = load_settings()
    mode = mode or current_mode(settings)
    cfg = get_databricks_config(settings, mode)
    conn = _get_connection(mode)
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW TABLES IN {cfg.get('catalog','main')}.{cfg.get('schema','default')}")
        rows = cur.fetchall()
        cur.close()
        names = [r[1] for r in rows]
        if layer:
            prefix = f"{layer}__"
            return [n[len(prefix):] for n in names if n.startswith(prefix)]
        return names
    finally:
        conn.close()


def run_sql(query: str, mode: Optional[str] = None) -> pd.DataFrame:
    conn = _get_connection(mode)
    try:
        cur = conn.cursor()
        cur.execute(query)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def execute_ddl(statement: str, mode: Optional[str] = None) -> None:
    """For DDL statements with no result set (CREATE VOLUME, CREATE SCHEMA, etc.) —
    run_sql assumes a result set and breaks on these."""
    conn = _get_connection(mode)
    try:
        cur = conn.cursor()
        cur.execute(statement)
        cur.close()
    finally:
        conn.close()


def test_connection(mode: Optional[str] = None) -> tuple[bool, str]:
    try:
        conn = _get_connection(mode)
        cur = conn.cursor()

        cur.execute("SELECT current_user()")
        user = cur.fetchall()[0][0]

        cur.close()
        conn.close()

        return True, f"Connected successfully as {user}"

    except Exception as e:
        import traceback
        return False, traceback.format_exc()