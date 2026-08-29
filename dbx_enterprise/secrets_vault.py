"""
dbx_enterprise/secrets_vault.py
---------------------------------
Real secret storage via Databricks Secret Scopes, for when this app is deployed on
Databricks Apps — replacing the plaintext database/app_settings.json for anything
sensitive (tokens, API keys).

IMPORTANT constraint (this is a real Databricks limitation, not a shortcut I'm taking):
the Secrets REST API can WRITE a secret value and LIST key names, but there is no API
to READ a secret's value back for a plain PAT-authenticated external caller — that's
deliberate, by design, to stop secrets leaking through logs/responses. The only way
code can read an actual secret value is `dbutils.secrets.get()`, and that only works
running ON Databricks compute (a notebook/job), not in this Streamlit process.

So the real, correct flow for Databricks Apps is:
1. Settings page here pushes a value to the Secret Scope via `put_secret` (write-only).
2. The workspace admin adds a resource binding in the app's `app.yaml` (or the Apps UI)
   mapping that scope+key to an environment variable name — this is a Databricks Apps
   platform feature, not something this code can do for itself; it requires a redeploy.
3. On the next app start, Databricks Apps injects that secret as a real env var, and
   config/settings.py's existing `os.getenv(...)` defaults (already used for every field)
   pick it up automatically — no new read path needed, since that mechanism already exists.

This module handles step 1 and tells you exactly what to do for step 2. It does NOT
pretend to do step 3 via REST, because that's not possible.

When DATABRICKS_APP_NAME isn't set (local dev, plain Streamlit hosting), this module
no-ops entirely and the plaintext settings file stays the source of truth — so nothing
changes for the Demo Mode / local-dev workflow that already works today.
"""
from __future__ import annotations

import os

import requests

REQUEST_TIMEOUT = 15
SCOPE_NAME = "datadoctorai"

# (settings path) -> (secret key, suggested env var name for the app.yaml binding)
SECRET_FIELDS = {
    ("databricks", "demo", "token"): ("databricks-demo-token", "DATABRICKS_DEMO_TOKEN"),
    ("databricks", "enterprise", "token"): ("databricks-enterprise-token", "DATABRICKS_TOKEN"),
    ("ai", "gemini", "api_key"): ("ai-gemini-key", "GEMINI_API_KEY"),
    ("ai", "openrouter", "api_key"): ("ai-openrouter-key", "OPENROUTER_API_KEY"),
    ("ai", "openai", "api_key"): ("ai-openai-key", "OPENAI_API_KEY"),
    ("ai", "azure_openai", "api_key"): ("ai-azure-openai-key", "AZURE_OPENAI_KEY"),
    ("ai", "claude", "api_key"): ("ai-claude-key", "CLAUDE_API_KEY"),
}


def is_available() -> bool:
    """True only when running as a deployed Databricks App."""
    return bool(os.environ.get("DATABRICKS_APP_NAME"))


def _host_and_token() -> tuple[str, str]:
    # Databricks Apps injects these automatically for the app's own service principal —
    # NOT the same as the workspace credentials a user enters in Settings.
    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not (host and token):
        raise RuntimeError(
            "Running as a Databricks App but DATABRICKS_HOST/DATABRICKS_TOKEN aren't set "
            "in the app environment — the platform should inject these for the app's "
            "service principal automatically; check the app's compute configuration."
        )
    return host.rstrip("/"), token


def ensure_scope() -> None:
    host, token = _host_and_token()
    r = requests.post(
        f"{host}/api/2.0/secrets/scopes/create",
        headers={"Authorization": f"Bearer {token}"},
        json={"scope": SCOPE_NAME, "scope_backend_type": "DATABRICKS"},
        timeout=REQUEST_TIMEOUT,
    )
    if not r.ok and "RESOURCE_ALREADY_EXISTS" not in r.text:
        raise RuntimeError(f"Could not create secret scope: HTTP {r.status_code}: {r.text}")


def put_secret(key: str, value: str) -> None:
    if not value:
        return
    host, token = _host_and_token()
    r = requests.post(
        f"{host}/api/2.0/secrets/put",
        headers={"Authorization": f"Bearer {token}"},
        json={"scope": SCOPE_NAME, "key": key, "string_value": value},
        timeout=REQUEST_TIMEOUT,
    )
    if not r.ok:
        raise RuntimeError(f"Could not write secret '{key}': HTTP {r.status_code}: {r.text}")


def list_secret_keys() -> list[str]:
    """Existence-only — Secrets has no value-read API, see module docstring."""
    host, token = _host_and_token()
    r = requests.get(
        f"{host}/api/2.0/secrets/list",
        headers={"Authorization": f"Bearer {token}"},
        params={"scope": SCOPE_NAME},
        timeout=REQUEST_TIMEOUT,
    )
    if not r.ok:
        return []
    return [s["key"] for s in r.json().get("secrets", [])]


def push_all_configured_secrets(settings: dict) -> list[str]:
    """Admin action (a Settings-page button, not automatic on every save): pushes every
    non-empty secret field currently in the plaintext settings up to the Secret Scope.
    Returns the list of (secret_key, env_var) binding instructions still needed to
    actually put them to use — this does NOT and CANNOT wire the app to read them back;
    that step is a manual app.yaml change + redeploy, by Databricks' own design."""
    if not is_available():
        raise RuntimeError("Not running as a Databricks App — no DATABRICKS_APP_NAME in environment.")
    ensure_scope()
    pushed = []
    for path, (secret_key, env_var) in SECRET_FIELDS.items():
        node = settings
        for p in path[:-1]:
            node = node.get(p, {})
        value = node.get(path[-1], "")
        if value:
            put_secret(secret_key, value)
            pushed.append(f"{secret_key} -> bind to env var {env_var} in app.yaml")
    return pushed
