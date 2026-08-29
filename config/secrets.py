"""
config/secrets.py
------------------
Small helpers for handling secret values safely in the UI (masking on display) and
resolving the "effective" value for a secret field: what's saved in settings takes
priority, otherwise fall back to the environment variable.

Secrets are stored locally in database/app_settings.json (gitignored). For real
production deployments, swap `load_settings`/`save_settings` for a proper secret
manager (Databricks Secret Scopes, Azure Key Vault, etc.) — the rest of the app only
talks to config.settings, so that's the only file you'd need to change.
"""
from typing import Optional


def mask(value: Optional[str], keep: int = 4) -> str:
    if not value:
        return ""
    if len(value) <= keep:
        return "•" * len(value)
    return value[:keep] + "•" * max(4, len(value) - keep)


def is_set(value: Optional[str]) -> bool:
    return bool(value and value.strip())
