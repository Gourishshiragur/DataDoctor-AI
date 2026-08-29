"""
config/enterprise.py
---------------------
Flags and helpers describing what "Enterprise Mode" unlocks once Databricks + Unity Catalog
are configured. The UI reads this to decide whether to show Databricks-backed panels
(job runs, cluster status, Unity Catalog lineage) instead of the local DuckDB equivalents.

No code changes are required to move between modes — every pipeline/ai module accepts an
`engine` argument that is either "duckdb" (demo) or "databricks" (enterprise) and both
paths implement the same interface.
"""
from config.settings import current_mode, get_databricks_config, is_databricks_configured, load_settings

ENTERPRISE_FEATURES = {
    "unity_catalog_lineage": "Full column-level lineage tracked in Unity Catalog instead of the local lineage.json log.",
    "databricks_jobs": "Bronze/Silver/Gold materialize as real Databricks Jobs on your workspace cluster/warehouse.",
    "shared_storage": "Bronze/Silver/Gold tables land in your configured ADLS/S3 location via Unity Catalog external locations.",
    "multi_user": "Settings, run history, and datasets are shared across your workspace users instead of local-only.",
    "paid_ai_providers": "OpenAI/Azure OpenAI/Claude activate on top of the free-tier providers (Ollama/Gemini/OpenRouter) already usable in Demo Mode.",
}


def enterprise_status() -> dict:
    settings = load_settings()
    mode = current_mode(settings)
    ready = is_databricks_configured(settings, mode)
    db = get_databricks_config(settings, mode)
    return {
        "ready": ready,
        "mode": mode,
        "workspace_url": db.get("workspace_url", ""),
        "catalog": db.get("catalog", ""),
        "schema": db.get("schema", ""),
        "features": ENTERPRISE_FEATURES,
    }
