"""
storage/db.py
-------------
Local storage engine used in Demo Mode. Provides the same interface the pipeline
modules use regardless of mode:

    write_table(layer, name, df)
    read_table(layer, name) -> DataFrame
    list_tables(layer) -> [names]
    table_exists(layer, name) -> bool

In Demo Mode this is backed by a single DuckDB file (storage/datadoctor.duckdb).
In Enterprise Mode, pipeline modules instead call databricks/connection.py, which
implements the identical function signatures against Unity Catalog tables — so
nothing upstream needs to change when you switch modes.
"""
from __future__ import annotations

from pathlib import Path
from typing import List

import duckdb
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = ROOT_DIR / "storage" / "datadoctor.duckdb"

VALID_LAYERS = ("bronze", "silver", "gold")


def _conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def _qualified(layer: str, name: str) -> str:
    assert layer in VALID_LAYERS, f"invalid layer {layer}"
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    return f"{layer}__{safe_name}"


def write_table(layer: str, name: str, df: pd.DataFrame) -> str:
    table = _qualified(layer, name)
    con = _conn()
    try:
        con.register("tmp_df", df)
        con.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM tmp_df')
    finally:
        con.close()
    return table


def read_table(layer: str, name: str) -> pd.DataFrame:
    table = _qualified(layer, name)
    con = _conn()
    try:
        return con.execute(f'SELECT * FROM "{table}"').df()
    finally:
        con.close()


def table_exists(layer: str, name: str) -> bool:
    table = _qualified(layer, name)
    con = _conn()
    try:
        res = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()
        return bool(res and res[0] > 0)
    finally:
        con.close()


def list_tables(layer: str | None = None) -> List[str]:
    con = _conn()
    try:
        rows = con.execute("SELECT table_name FROM information_schema.tables").fetchall()
    finally:
        con.close()
    names = [r[0] for r in rows]
    if layer:
        prefix = f"{layer}__"
        return [n[len(prefix):] for n in names if n.startswith(prefix)]
    return names


def run_sql(query: str) -> pd.DataFrame:
    """Run an arbitrary read-only SQL query against the local warehouse (used by SQL Generator)."""
    con = _conn()
    try:
        return con.execute(query).df()
    finally:
        con.close()
