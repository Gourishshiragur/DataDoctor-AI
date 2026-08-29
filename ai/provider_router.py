"""
ai/provider_router.py
----------------------
Selects the first configured AI provider (priority order: Ollama > Gemini > OpenRouter >
OpenAI > Azure OpenAI > Claude) and routes a (system, prompt) pair to it.

If NO provider is configured/reachable, falls back to `offline_infer`, a deterministic
rule-based responder that still powers real functionality (repair suggestions, SQL
generation, quality summaries) without ever calling out to the network. This is what
lets "AI pipeline generation" work out of the box in Demo Mode with zero keys.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import requests

from ai import gemini, ollama, openrouter
from config.settings import PROVIDER_PRIORITY, configured_providers, current_mode, load_settings


@dataclass
class AIResponse:
    text: str
    provider: str
    ok: bool
    error: Optional[str] = None


def _call_openai(api_key: str, model: str, prompt: str, system: str, timeout=60.0) -> str:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}
    ]
    r = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers=headers,
        json={"model": model, "messages": messages},
        timeout=timeout,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def _call_azure(endpoint: str, deployment: str, api_version: str, api_key: str, prompt: str, system: str, timeout=60.0) -> str:
    url = f"{endpoint.rstrip('/')}/openai/deployments/{deployment}/chat/completions?api-version={api_version}"
    headers = {"api-key": api_key, "Content-Type": "application/json"}
    messages = ([{"role": "system", "content": system}] if system else []) + [
        {"role": "user", "content": prompt}
    ]
    r = requests.post(url, headers=headers, json={"messages": messages}, timeout=timeout)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def _call_claude(api_key: str, model: str, prompt: str, system: str, timeout=60.0) -> str:
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "max_tokens": 1024,
        "system": system or "",
        "messages": [{"role": "user", "content": prompt}],
    }
    r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text").strip()


def get_active_provider() -> str:
    settings = load_settings()
    providers = configured_providers(settings, mode=current_mode(settings))
    return providers[0] if providers else "offline"


def infer(prompt: str, system: str = "", prefer: Optional[str] = None) -> AIResponse:
    """Route a prompt to the best available provider. Never raises — always returns an AIResponse."""
    settings = load_settings()
    providers = configured_providers(settings, mode=current_mode(settings))
    order = [prefer] + [p for p in providers if p != prefer] if prefer else providers

    last_error = None
    for provider in order:
        if not provider:
            continue
        try:
            ai = settings["ai"]
            if provider == "ollama":
                cfg = ai["ollama"]
                if not ollama.is_reachable(cfg["url"]):
                    continue
                text = ollama.complete(cfg["url"], cfg["model"], prompt, system)
            elif provider == "gemini":
                cfg = ai["gemini"]
                text = gemini.complete(cfg["api_key"], cfg["model"], prompt, system)
            elif provider == "openrouter":
                cfg = ai["openrouter"]
                text = openrouter.complete(cfg["api_key"], cfg["model"], prompt, system)
            elif provider == "openai":
                cfg = ai["openai"]
                text = _call_openai(cfg["api_key"], cfg["model"], prompt, system)
            elif provider == "azure_openai":
                cfg = ai["azure_openai"]
                text = _call_azure(cfg["endpoint"], cfg["deployment"], cfg["api_version"], cfg["api_key"], prompt, system)
            elif provider == "claude":
                cfg = ai["claude"]
                text = _call_claude(cfg["api_key"], cfg["model"], prompt, system)
            else:
                continue
            if text:
                return AIResponse(text=text, provider=provider, ok=True)
        except Exception as e:  # noqa: BLE001
            last_error = f"{provider}: {e}"
            continue

    # Nothing configured or all failed -> offline rule-based fallback
    text = offline_infer(prompt, system)
    return AIResponse(text=text, provider="offline", ok=True, error=last_error)


# ----------------------------------------------------------------------------------
# Offline fallback: deterministic, rule-based "AI" so the platform is 100% functional
# with zero API keys. Real natural-language provider replaces this the moment one is
# configured — same call site, same return shape.
# ----------------------------------------------------------------------------------
def offline_infer(prompt: str, system: str = "") -> str:
    p = prompt.lower()

    if "sql" in system.lower() or "sql" in p[:60]:
        return _offline_sql(prompt)
    if "repair" in system.lower() or "fix" in p[:80]:
        return _offline_repair_note(prompt)
    if "business" in system.lower() or "insight" in system.lower():
        return _offline_business_note(prompt)
    return (
        "Offline mode: no AI provider is configured, so this is a rule-based response. "
        "Connect Ollama, Gemini, OpenRouter, OpenAI, Azure OpenAI, or Claude in Settings "
        "for full natural-language responses."
    )


def _offline_sql(prompt: str) -> str:
    m = re.search(r"table[s]?\s*[:\-]?\s*([a-zA-Z0-9_]+)", prompt, re.I)
    table = m.group(1) if m else "gold_table"
    if any(k in prompt.lower() for k in ["top", "highest", "most"]):
        return f"SELECT * FROM {table} ORDER BY 2 DESC LIMIT 10;  -- offline heuristic: adjust column"
    if any(k in prompt.lower() for k in ["count", "how many"]):
        return f"SELECT COUNT(*) AS row_count FROM {table};"
    if any(k in prompt.lower() for k in ["average", "avg", "mean"]):
        return f"SELECT AVG(*) FROM {table};  -- offline heuristic: replace * with a numeric column"
    return f"SELECT * FROM {table} LIMIT 100;"


def _offline_repair_note(prompt: str) -> str:
    return (
        "Rule-based repair suggestion applied: nulls imputed with median/mode, duplicates "
        "dropped by primary key, and out-of-range numeric values capped to the 1st/99th "
        "percentile. Connect an AI provider for context-aware repair explanations."
    )


def _offline_business_note(prompt: str) -> str:
    return (
        "Offline summary: based on the Gold layer aggregates, review the Dashboard's KPI "
        "cards and trend chart for the current numbers. Connect an AI provider in Settings "
        "for a narrative business summary."
    )
