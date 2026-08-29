"""ai/ollama.py — thin client for a local Ollama server. No API key needed."""
from __future__ import annotations

import requests


def is_reachable(url: str, timeout: float = 1.5) -> bool:
    try:
        r = requests.get(f"{url.rstrip('/')}/api/tags", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def complete(url: str, model: str, prompt: str, system: str = "", timeout: float = 60.0) -> str:
    payload = {
        "model": model,
        "prompt": prompt,
        "system": system,
        "stream": False,
    }
    r = requests.post(f"{url.rstrip('/')}/api/generate", json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data.get("response", "").strip()
