"""pipeline/profiling.py — computes per-column profiling stats used to drive
quality checks, repair suggestions, and the Dashboard's data health view."""
from __future__ import annotations

import numpy as np
import pandas as pd


def profile_dataframe(df: pd.DataFrame) -> dict:
    profile = {"row_count": len(df), "column_count": df.shape[1], "columns": {}}
    for col in df.columns:
        s = df[col]
        col_profile = {
            "dtype": str(s.dtype),
            "null_count": int(s.isna().sum()),
            "null_pct": round(float(s.isna().mean() * 100), 2),
            "unique_count": int(s.nunique(dropna=True)),
            "duplicate_count": 0,
        }
        if pd.api.types.is_numeric_dtype(s):
            clean = s.dropna()
            if len(clean) > 0:
                q1, q99 = clean.quantile(0.01), clean.quantile(0.99)
                col_profile.update({
                    "min": float(clean.min()),
                    "max": float(clean.max()),
                    "mean": float(clean.mean()),
                    "std": float(clean.std()) if len(clean) > 1 else 0.0,
                    "outlier_count": int(((clean < q1) | (clean > q99)).sum()),
                })
        else:
            top = s.dropna().value_counts().head(3).to_dict()
            col_profile["top_values"] = {str(k): int(v) for k, v in top.items()}
        profile["columns"][col] = col_profile

    profile["duplicate_rows"] = int(df.duplicated().sum())
    profile["overall_null_pct"] = round(float(df.isna().mean().mean() * 100), 2)
    return profile


def quality_score(profile: dict) -> float:
    """A simple 0-100 composite score: penalize nulls, duplicates, and outliers."""
    n_cols = max(profile["column_count"], 1)
    avg_null_pct = np.mean([c["null_pct"] for c in profile["columns"].values()]) if profile["columns"] else 0
    dup_pct = (profile["duplicate_rows"] / profile["row_count"] * 100) if profile["row_count"] else 0
    outlier_penalty = np.mean(
        [c.get("outlier_count", 0) / max(profile["row_count"], 1) * 100 for c in profile["columns"].values()]
    ) if profile["columns"] else 0

    score = 100 - (avg_null_pct * 0.5) - (dup_pct * 1.0) - (outlier_penalty * 0.5)
    return round(max(0.0, min(100.0, score)), 1)
