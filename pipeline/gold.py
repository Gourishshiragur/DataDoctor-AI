"""pipeline/gold.py — Gold layer: business-ready aggregates.

Generic auto-aggregation: groups by the first low-cardinality categorical column found
and computes sum/mean/count over numeric columns. This keeps Gold generation dataset-
agnostic (works for retail/banking/healthcare/ecommerce/manufacturing alike) while still
producing genuinely useful KPI tables for the Dashboard/Business AI pages.
"""
from __future__ import annotations

import pandas as pd

from pipeline import lineage
from storage import router as db


def _pick_group_column(df: pd.DataFrame) -> str | None:
    candidates = [
        c for c in df.select_dtypes(include="object").columns
        if 2 <= df[c].nunique(dropna=True) <= 30
    ]
    return candidates[0] if candidates else None


def build(silver_df: pd.DataFrame, dataset_name: str, run_id: str) -> dict:
    numeric_cols = silver_df.select_dtypes(include="number").columns.tolist()
    group_col = _pick_group_column(silver_df)

    kpis = {
        "total_rows": int(len(silver_df)),
    }
    for col in numeric_cols[:4]:
        kpis[f"sum_{col}"] = round(float(silver_df[col].sum()), 2)
        kpis[f"avg_{col}"] = round(float(silver_df[col].mean()), 2)

    if group_col and numeric_cols:
        agg_dict = {c: ["sum", "mean", "count"] for c in numeric_cols[:4]}
        gold_df = silver_df.groupby(group_col).agg(agg_dict)
        gold_df.columns = ["_".join(c).strip() for c in gold_df.columns]
        gold_df = gold_df.reset_index()
    elif numeric_cols:
        gold_df = silver_df[numeric_cols].describe().reset_index().rename(columns={"index": "statistic"})
    else:
        gold_df = silver_df.copy()

    db.write_table("gold", dataset_name, gold_df, run_id=run_id)
    lineage.record(run_id, "silver", dataset_name, "gold", dataset_name, "business aggregation")

    return {"df": gold_df, "kpis": kpis, "group_column": group_col}
