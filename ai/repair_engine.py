"""
ai/repair_engine.py
--------------------
The "self-healing" core of DataDoctorAI. Given a raw DataFrame and the quality check
results from pipeline/quality.py, this module automatically repairs the data and returns:

- the repaired DataFrame
- a list of repair actions taken (for the lineage/repair log + UI "live process" view)

Repair strategy is rule-based (deterministic, auditable) by default, and optionally
enriched with a one-line AI-generated justification per repair via ai/provider_router
when a provider is configured (falls back to a canned rationale otherwise — never blocks).
"""
from __future__ import annotations

import pandas as pd

from ai import prompt_library
from ai.provider_router import infer

NEGATIVE_INVALID_HINTS = ("quantity", "age", "amount", "balance", "price")


def _explain(column: str, dtype: str, issue: str, samples: list) -> str:
    resp = infer(
        prompt_library.repair_prompt(column, dtype, issue, samples),
        system=prompt_library.REPAIR_SYSTEM_PROMPT,
    )
    return resp.text


def repair(df: pd.DataFrame, explain: bool = True) -> tuple[pd.DataFrame, list[dict]]:
    repaired = df.copy()
    actions: list[dict] = []

    # 1) Null imputation: median for numeric, mode for categorical
    for col in repaired.columns:
        null_count = int(repaired[col].isna().sum())
        if null_count == 0:
            continue
        if pd.api.types.is_numeric_dtype(repaired[col]):
            fill_value = repaired[col].median()
            strategy = f"median ({round(fill_value, 2) if pd.notna(fill_value) else 'n/a'})"
        else:
            mode_vals = repaired[col].mode(dropna=True)
            fill_value = mode_vals.iloc[0] if not mode_vals.empty else ""
            strategy = f"mode ('{fill_value}')"
        if pd.isna(fill_value):
            continue
        repaired[col] = repaired[col].fillna(fill_value)
        action = {
            "column_name": col,
            "issue": f"{null_count} null value(s)",
            "action": f"Imputed with {strategy}",
            "rows_affected": null_count,
            "source": "rule",
        }
        if explain:
            action["explanation"] = _explain(col, str(df[col].dtype), "null values", [])
        actions.append(action)

    # 3) Negative-value correction for columns that should never be negative
    for col in repaired.select_dtypes(include="number").columns:
        if not any(hint in col.lower() for hint in NEGATIVE_INVALID_HINTS):
            continue
        neg_mask = repaired[col] < 0
        neg_count = int(neg_mask.sum())
        if neg_count == 0:
            continue
        median_positive = repaired.loc[~neg_mask, col].median()
        repaired.loc[neg_mask, col] = median_positive if pd.notna(median_positive) else 0
        actions.append({
            "column_name": col,
            "issue": f"{neg_count} invalid negative value(s)",
            "action": f"Replaced with median positive value ({round(median_positive, 2) if pd.notna(median_positive) else 0})",
            "rows_affected": neg_count,
            "source": "rule",
        })

    # 4) Outlier capping (winsorize at 1st/99th percentile)
    for col in repaired.select_dtypes(include="number").columns:
        clean = repaired[col].dropna()
        if len(clean) < 10:
            continue
        q1, q99 = clean.quantile(0.01), clean.quantile(0.99)
        mask = (repaired[col] < q1) | (repaired[col] > q99)
        n = int(mask.sum())
        if n == 0:
            continue
        repaired[col] = repaired[col].clip(lower=q1, upper=q99)
        actions.append({
            "column_name": col,
            "issue": f"{n} outlier value(s) beyond 1st/99th percentile",
            "action": f"Capped to range [{round(q1,2)}, {round(q99,2)}]",
            "rows_affected": n,
            "source": "rule",
        })

    # 5) Categorical normalization (trim + lowercase then title-case canonical form)
    for col in repaired.select_dtypes(include="object").columns:
        raw_vals = repaired[col].dropna().unique()
        if len(raw_vals) == 0 or len(raw_vals) > 50:
            continue
        canon_map = {v: str(v).strip().title() for v in raw_vals}
        distinct_before = len(raw_vals)
        distinct_after = len(set(canon_map.values()))
        if distinct_after < distinct_before:
            affected = int(repaired[col].isin([v for v in raw_vals if canon_map[v] != v]).sum())
            repaired[col] = repaired[col].map(lambda v: canon_map.get(v, v))
            actions.append({
                "column_name": col,
                "issue": f"{distinct_before} inconsistent case/whitespace variants",
                "action": f"Normalized to {distinct_after} canonical value(s)",
                "rows_affected": affected,
                "source": "rule",
            })

    # 6) Drop exact duplicate rows — run LAST, since imputation/normalization above can
    # themselves collapse previously-distinct rows into duplicates.
    dup_count = int(repaired.duplicated().sum())
    if dup_count > 0:
        repaired = repaired.drop_duplicates()
        actions.append({
            "column_name": "*",
            "issue": "duplicate_rows",
            "action": f"Dropped {dup_count} exact duplicate row(s)",
            "rows_affected": dup_count,
            "source": "rule",
        })

    return repaired, actions
