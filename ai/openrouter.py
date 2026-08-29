"""ai/openrouter.py — thin client for OpenRouter's OpenAI-compatible chat completions API."""
from __future__ import annotations

import requests

BASE = "https://openrouter.ai/api/v1/chat/completions"


def complete(api_key: str, model: str, prompt: str, system: str = "", timeout: float = 60.0) -> str:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    payload = {"model": model, "messages": messages}
    r = requests.post(BASE, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"].strip()
