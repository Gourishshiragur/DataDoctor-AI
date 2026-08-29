"""
config/settings.py
-------------------
Central settings loader/saver for DataDoctorAI.

Design goals:
- Users enter keys ONCE via the Settings page (ui/Settings.py).
- Settings persist to a local JSON file (database/app_settings.json) so they survive restarts.
- Environment variables (.env) act as the initial defaults / fallback for headless deployments.
- Nothing here requires any external service — the app works fully in Demo Mode with an
  empty settings file.
"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parent.parent
SETTINGS_PATH = ROOT_DIR / "database" / "app_settings.json"

DEFAULT_SETTINGS: Dict[str, Any] = {
    "mode": "demo",  # "demo" or "enterprise" — user-pinned toggle in Settings
    "databricks": {
        # Separate credential slots: Demo Mode is meant to run against YOUR OWN
        # Databricks Free Edition workspace (zero cost); Enterprise Mode is meant to
        # run against a customer's paid/production workspace. Kept as two independent
        # slots so switching modes never mixes the two up or requires re-entering keys.
        "demo": {
            "workspace_url": os.getenv("DATABRICKS_DEMO_HOST", ""),
            "token": os.getenv("DATABRICKS_DEMO_TOKEN", ""),
            "http_path": os.getenv("DATABRICKS_DEMO_HTTP_PATH", ""),
            "cluster_id": os.getenv("DATABRICKS_DEMO_CLUSTER_ID", ""),
            "catalog": os.getenv("DATABRICKS_DEMO_CATALOG", "main"),
            "schema": os.getenv("DATABRICKS_DEMO_SCHEMA", "demo"),
        },
        "enterprise": {
            "workspace_url": os.getenv("DATABRICKS_HOST", ""),
            "token": os.getenv("DATABRICKS_TOKEN", ""),
            "http_path": os.getenv("DATABRICKS_HTTP_PATH", ""),
            "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", ""),
            "catalog": os.getenv("DATABRICKS_CATALOG", "main"),
            "schema": os.getenv("DATABRICKS_SCHEMA", "enterprise"),
        },
    },
    "pipeline": {
        # Consumed by the native Databricks Job path (dbx_enterprise/jobs.py + the
        # notebook) — table names, retry behavior, and Delta housekeeping for the
        # Spark execution path, as opposed to the in-app pandas pipeline above it.
        "bronze_table": "bronze", "silver_table": "silver", "gold_table": "gold",
        "batch_size": 50000,
        "max_retries": 2, "retry_delay_seconds": 3,
        "optimize_after_load": True, "vacuum_after_load": False,
    },
    "quality": {
        "minimum_score": 70,        # below this, Silver flags the dataset for review
        "duplicate_threshold": 0.05,  # fraction of duplicate rows tolerated before flagging
        "null_threshold": 0.30,       # fraction of nulls in a column before flagging it
        "schema_validation": True,
        "business_validation": True,
    },
    "ai": {
        "routing": {
            # "auto" walks PROVIDER_PRIORITY; "manual" forces a single provider per mode
            # if it's configured and allowed, else falls back to the offline engine.
            "mode": "auto",
            "demo_provider": "",
            "enterprise_provider": "",
        },
        "ollama": {
            "url": os.getenv("OLLAMA_URL", "http://localhost:11434"),
            "model": os.getenv("OLLAMA_MODEL", "llama3.1"),
        },
        "gemini": {
            "api_key": os.getenv("GEMINI_API_KEY", ""),
            "model": os.getenv("GEMINI_MODEL", "gemini-1.5-flash"),
        },
        "openrouter": {
            "api_key": os.getenv("OPENROUTER_API_KEY", ""),
            "model": os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.1-8b-instruct:free"),
        },
        "openai": {
            "api_key": os.getenv("OPENAI_API_KEY", ""),
            "model": os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
        },
        "azure_openai": {
            "endpoint": os.getenv("AZURE_OPENAI_ENDPOINT", ""),
            "deployment": os.getenv("AZURE_OPENAI_DEPLOYMENT", ""),
            "api_version": os.getenv("AZURE_OPENAI_API_VERSION", "2024-05-01-preview"),
            "api_key": os.getenv("AZURE_OPENAI_KEY", ""),
        },
        "claude": {
            "api_key": os.getenv("CLAUDE_API_KEY", ""),
            "model": os.getenv("CLAUDE_MODEL", "claude-sonnet-4-6"),
        },
        "rag": {"top_k": 5},
    },
    "monitoring": {
        "history_days": 30,   # Monitor page only shows runs/backend events within this window
        "audit_logging": True,  # when True, backend/run events also record fuller detail
    },
    "features": {
        # Page-level visibility toggles for app.py's nav — lets an operator trim the
        # app down (e.g. hide Business AI if no AI keys are meant to be used at all).
        "pipeline_builder": True,
        "business_ai": True,
        "pipeline_monitor": True,
        "dashboard": True,
    },
}

# Providers with a genuine no-cost tier (no billing account required to obtain a key):
# Ollama is free because it's self-hosted; Gemini and OpenRouter both offer real
# permanently-free API tiers/models. OpenAI, Azure OpenAI, and Claude have no standing
# free tier (trial credits only), so they stay Enterprise-only to avoid a demo silently
# running up a bill.
FREE_TIER_AI_PROVIDERS = ["ollama", "gemini", "openrouter"]
PROVIDER_PRIORITY = ["ollama", "gemini", "openrouter", "openai", "azure_openai", "claude"]


def _deep_merge(base: dict, override: dict) -> dict:
    result = deepcopy(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(result.get(k), dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_settings() -> Dict[str, Any]:
    """Load persisted settings, merged over defaults. Never raises."""
    if SETTINGS_PATH.exists():
        try:
            with open(SETTINGS_PATH, "r") as f:
                saved = json.load(f)
            return _deep_merge(DEFAULT_SETTINGS, saved)
        except Exception:
            pass
    return deepcopy(DEFAULT_SETTINGS)


def save_settings(settings: Dict[str, Any]) -> None:
    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2)


def get_databricks_config(settings: Dict[str, Any], mode: Optional[str] = None) -> Dict[str, Any]:
    """Returns the Demo-workspace or Enterprise-workspace credential slot, whichever
    matches `mode` (or the currently active mode if not given)."""
    mode = mode or current_mode(settings)
    return settings.get("databricks", {}).get(mode, {})


def is_databricks_configured(settings: Dict[str, Any], mode: Optional[str] = None) -> bool:
    db = get_databricks_config(settings, mode)
    return bool(db.get("workspace_url") and db.get("token") and (db.get("http_path") or db.get("cluster_id")))


def configured_providers(settings: Dict[str, Any], mode: Optional[str] = None) -> list[str]:
    """Return providers that have enough config to be usable, in priority order.

    Mode-aware: Demo Mode is restricted to providers with a genuine no-cost tier
    (Ollama, Gemini, OpenRouter) even if paid keys happen to be saved in Settings —
    those only activate once the user switches to Enterprise Mode. Mirrors the same
    free-by-default / paid-when-enterprise split as the storage layer.
    """
    if mode is None:
        mode = current_mode(settings)
    ai = settings.get("ai", {})
    out = []
    if ai.get("ollama", {}).get("url"):
        out.append("ollama")  # ollama needs no key; presence of URL is enough to *try*
    if ai.get("gemini", {}).get("api_key"):
        out.append("gemini")
    if ai.get("openrouter", {}).get("api_key"):
        out.append("openrouter")
    if mode == "enterprise":
        if ai.get("openai", {}).get("api_key"):
            out.append("openai")
        az = ai.get("azure_openai", {})
        if az.get("endpoint") and az.get("api_key") and az.get("deployment"):
            out.append("azure_openai")
        if ai.get("claude", {}).get("api_key"):
            out.append("claude")
    else:
        out = [p for p in out if p in FREE_TIER_AI_PROVIDERS]  # safety net

    routing = ai.get("routing", {})
    if routing.get("mode") == "manual":
        forced = routing.get("enterprise_provider" if mode == "enterprise" else "demo_provider")
        if forced and forced in out:
            return [forced]
        return []  # manual mode with nothing usable configured -> offline fallback

    # sort by global priority
    return [p for p in PROVIDER_PRIORITY if p in out]


def current_mode(settings: Dict[str, Any]) -> str:
    """Demo mode unless the user has pinned Enterprise."""
    return "enterprise" if settings.get("mode") == "enterprise" else "demo"


def get_pipeline_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    return settings.get("pipeline", DEFAULT_SETTINGS["pipeline"])


def get_quality_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    return settings.get("quality", DEFAULT_SETTINGS["quality"])


def get_monitoring_settings(settings: Dict[str, Any]) -> Dict[str, Any]:
    return settings.get("monitoring", DEFAULT_SETTINGS["monitoring"])


def get_feature_flags(settings: Dict[str, Any]) -> Dict[str, Any]:
    return settings.get("features", DEFAULT_SETTINGS["features"])
