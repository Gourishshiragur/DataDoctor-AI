"""pipeline/quality.py — rule-based data quality checks run against a layer.

Each check returns (passed: bool, details: dict). These are logged via
database.history.log_quality_check so the Monitor page can show pass/fail history.
"""
from __future__ import annotations

import pandas as pd

CHECKS = [
    "null_thresholds",
    "duplicate_rows",
    "negative_values_where_invalid",
    "outlier_ranges",
    "categorical_consistency",
]

NEGATIVE_INVALID_HINTS = ("quantity", "age", "amount", "balance", "price")


def check_null_thresholds(df: pd.DataFrame, threshold_pct: float = 15.0):
    null_pct = df.isna().mean() * 100
    offenders = null_pct[null_pct > threshold_pct]
    passed = offenders.empty
    return passed, {"threshold_pct": threshold_pct, "offending_columns": offenders.round(2).to_dict()}


def check_duplicate_rows(df: pd.DataFrame, threshold_fraction: float = 0.0):
    dup_count = int(df.duplicated().sum())
    dup_fraction = dup_count / len(df) if len(df) else 0.0
    return dup_fraction <= threshold_fraction, {"duplicate_rows": dup_count, "duplicate_fraction": round(dup_fraction, 4),
                                                  "threshold_fraction": threshold_fraction}


def check_negative_values(df: pd.DataFrame):
    offenders = {}
    for col in df.columns:
        if not any(hint in col.lower() for hint in NEGATIVE_INVALID_HINTS):
            continue
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        neg = int((df[col] < 0).sum())
        if neg > 0:
            offenders[col] = neg
    return len(offenders) == 0, {"offending_columns": offenders}


def check_outlier_ranges(df: pd.DataFrame):
    offenders = {}
    for col in df.select_dtypes(include="number").columns:
        clean = df[col].dropna()
        if len(clean) < 10:
            continue
        q1, q99 = clean.quantile(0.01), clean.quantile(0.99)
        n = int(((clean < q1) | (clean > q99)).sum())
        if n > 0:
            offenders[col] = n
    return len(offenders) == 0, {"offending_columns": offenders}


def check_categorical_consistency(df: pd.DataFrame):
    """Flags columns where case/whitespace variants collapse to fewer canonical values
    than raw distinct values (e.g. 'Savings' vs 'savings')."""
    offenders = {}
    for col in df.select_dtypes(include="object").columns:
        raw_vals = df[col].dropna().unique()
        if len(raw_vals) == 0 or len(raw_vals) > 50:
            continue
        canon = {str(v).strip().lower() for v in raw_vals}
        if len(canon) < len(raw_vals):
            offenders[col] = {"raw_variants": len(raw_vals), "canonical_variants": len(canon)}
    return len(offenders) == 0, {"offending_columns": offenders}


CHECK_FUNCS = {
    "null_thresholds": check_null_thresholds,
    "duplicate_rows": check_duplicate_rows,
    "negative_values_where_invalid": check_negative_values,
    "outlier_ranges": check_outlier_ranges,
    "categorical_consistency": check_categorical_consistency,
}

# Which settings["quality"] flag gates which check category. null/duplicate checks are
# structural (schema_validation); negative-value/categorical checks are business rules.
CHECK_CATEGORY = {
    "null_thresholds": "schema_validation",
    "duplicate_rows": "schema_validation",
    "outlier_ranges": "schema_validation",
    "negative_values_where_invalid": "business_validation",
    "categorical_consistency": "business_validation",
}


def run_all_checks(df: pd.DataFrame, quality_settings: dict | None = None) -> list[dict]:
    """`quality_settings` is settings["quality"] from config.settings.load_settings() —
    pass it through so thresholds/toggles set in Settings actually take effect instead
    of the hardcoded defaults."""
    qs = quality_settings or {}
    null_threshold_pct = qs.get("null_threshold", 0.30) * 100
    duplicate_threshold = qs.get("duplicate_threshold", 0.0)

    results = []
    for name, func in CHECK_FUNCS.items():
        category = CHECK_CATEGORY[name]
        if category == "schema_validation" and not qs.get("schema_validation", True):
            continue
        if category == "business_validation" and not qs.get("business_validation", True):
            continue
        if name == "null_thresholds":
            passed, details = func(df, threshold_pct=null_threshold_pct)
        elif name == "duplicate_rows":
            passed, details = func(df, threshold_fraction=duplicate_threshold)
        else:
            passed, details = func(df)
        results.append({"check_name": name, "passed": passed, "details": details})
    return results


def quality_score(results: list[dict]) -> int:
    """0-100 score from check results — used against quality.minimum_score to flag a run."""
    if not results:
        return 100
    return round(100 * sum(1 for r in results if r["passed"]) / len(results))
