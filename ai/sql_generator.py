"""ai/sql_generator.py — natural language -> SQL over a Gold/Silver table, executed
against the local DuckDB warehouse (Demo Mode) or Databricks SQL Warehouse (Enterprise Mode)."""
from __future__ import annotations

import re

import pandas as pd

from ai import prompt_library
from ai.provider_router import infer
from config.settings import current_mode, load_settings


def _schema_string(df: pd.DataFrame) -> str:
    return "\n".join(f"- {c} ({df[c].dtype})" for c in df.columns)


def _clean_sql(text: str) -> str:
    text = re.sub(r"```sql|```", "", text).strip()
    return text


def generate_sql(question: str, df: pd.DataFrame, table_name: str) -> dict:
    schema = f"Table name: {table_name}\n" + _schema_string(df)
    resp = infer(
        prompt_library.sql_prompt(question, schema),
        system=prompt_library.SQL_SYSTEM_PROMPT,
    )
    sql = _clean_sql(resp.text)
    return {"sql": sql, "provider": resp.provider}


def run_query(sql: str, layer_df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """Executes the generated SQL against a fresh in-memory DuckDB registered with the
    dataframe under `table_name`, regardless of which mode produced the dataframe."""
    import duckdb

    con = duckdb.connect(":memory:")
    con.register(table_name, layer_df)
    try:
        return con.execute(sql).df()
    finally:
        con.close()
