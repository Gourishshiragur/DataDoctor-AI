"""ai/gemini.py — thin REST client for Google Gemini (no SDK dependency required)."""
from __future__ import annotations

import requests

BASE = "https://generativelanguage.googleapis.com/v1beta/models"


def complete(api_key: str, model: str, prompt: str, system: str = "", timeout: float = 60.0) -> str:
    url = f"{BASE}/{model}:generateContent?key={api_key}"
    contents = []
    if system:
        contents.append({"role": "user", "parts": [{"text": f"[System instructions]\n{system}"}]})
    contents.append({"role": "user", "parts": [{"text": prompt}]})
    payload = {"contents": contents}
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    candidates = data.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return "".join(p.get("text", "") for p in parts).strip()
