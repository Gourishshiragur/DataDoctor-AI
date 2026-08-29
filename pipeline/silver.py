"""pipeline/silver.py — Silver layer: quality-checked, self-healed data.

Flow: Bronze df -> run quality checks -> self-healing repair engine -> re-check ->
persist to Silver + log lineage/quality/repair events for the given run_id.

Returns a rich result dict so the UI can show a live, step-by-step trace.
"""
from __future__ import annotations

import pandas as pd

from ai.repair_engine import repair
from config.settings import get_quality_settings, load_settings
from database import history
from pipeline import lineage, quality
from storage import router as db


def process(bronze_df: pd.DataFrame, dataset_name: str, run_id: str, explain: bool = True) -> dict:
    working_df = bronze_df.drop(columns=["_ingested_at", "_row_hash"], errors="ignore")
    quality_settings = get_quality_settings(load_settings())

    pre_checks = quality.run_all_checks(working_df, quality_settings)
    for c in pre_checks:
        history.log_quality_check(run_id, c["check_name"], "bronze", c["passed"], c["details"])

    repaired_df, actions = repair(working_df, explain=explain)
    for a in actions:
        history.log_repair(run_id, a["column_name"], a["issue"], a["action"], a["rows_affected"], a["source"])

    post_checks = quality.run_all_checks(repaired_df, quality_settings)
    for c in post_checks:
        history.log_quality_check(run_id, c["check_name"], "silver", c["passed"], c["details"])

    score = quality.quality_score(post_checks)
    below_minimum = score < quality_settings.get("minimum_score", 70)

    db.write_table("silver", dataset_name, repaired_df, run_id=run_id)
    lineage.record(run_id, "bronze", dataset_name, "silver", dataset_name, "self-healing repair engine")

    return {
        "df": repaired_df,
        "pre_checks": pre_checks,
        "post_checks": post_checks,
        "repair_actions": actions,
        "quality_score": score,
        "below_minimum": below_minimum,
    }
