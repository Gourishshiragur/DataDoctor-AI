"""pipeline/bronze.py — Bronze layer: raw, as-is ingestion.

Bronze never mutates values. It only:
- adds ingestion metadata (row hash, ingested_at, source_file)
- persists the raw frame exactly as uploaded
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import pandas as pd

from storage import router as db


def _row_hash(row: pd.Series) -> str:
    return hashlib.md5("|".join(str(v) for v in row.values).encode()).hexdigest()[:12]


def ingest(df: pd.DataFrame, dataset_name: str, run_id: str | None = None) -> pd.DataFrame:
    bronze_df = df.copy()
    bronze_df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    bronze_df["_row_hash"] = bronze_df.apply(_row_hash, axis=1)
    db.write_table("bronze", dataset_name, bronze_df, run_id=run_id)
    return bronze_df
