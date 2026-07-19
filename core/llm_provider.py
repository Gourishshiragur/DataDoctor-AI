"""
LLM Provider — three-tier hierarchy for DataDoctor AI.

Tier 1 — Claude API (Anthropic)
    Triggered when ANTHROPIC_API_KEY is set in Streamlit secrets or env.
    Best quality. Works on Streamlit Cloud with no local install needed.

Tier 2 — Ollama (local)
    Triggered when Ollama is running locally and a model is installed.
    Free, private, works offline. Does not work on Streamlit Cloud.

Tier 3 — Rule-based fallback
    Always works. No API key, no local server needed.
    Covers the most common Spark/Delta error patterns.

Usage:
    from core.llm_provider import llm_chat, llm_status

    response = llm_chat(prompt="Explain this error: ...", context="...")
    status   = llm_status()   # {"tier": 1, "provider": "Claude API", ...}
"""
from __future__ import annotations

import json
import os
from typing import Any, Dict, Generator, List, Optional

import requests

# ──────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-6"  # default; overridden by ANTHROPIC_MODEL secret/env if set
DEFAULT_OLLAMA_URL = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "llama3"
DEFAULT_GROQ_BASE_URL = "https://api.groq.com/openai/v1"
DEFAULT_GROQ_MODEL = "llama-3.3-70b-versatile"
REQUEST_TIMEOUT = 30
STREAM_TIMEOUT = 60


# ──────────────────────────────────────────────────────────────
# API key helpers
# ──────────────────────────────────────────────────────────────
BYOK_SESSION_KEY = "byok_anthropic_api_key"


def set_session_api_key(key: str) -> None:
    """
    Store a user-supplied Anthropic key for THIS browser session only.
    Never written to disk, never saved to store.py, never logged.
    Cleared automatically when the session ends.
    """
    try:
        import streamlit as st
        st.session_state[BYOK_SESSION_KEY] = (key or "").strip()
    except Exception:
        pass


def clear_session_api_key() -> None:
    try:
        import streamlit as st
        st.session_state.pop(BYOK_SESSION_KEY, None)
    except Exception:
        pass


def session_api_key_active() -> bool:
    try:
        import streamlit as st
        return bool(st.session_state.get(BYOK_SESSION_KEY))
    except Exception:
        return False


def _session_claude_api_key() -> Optional[str]:
    try:
        import streamlit as st
        key = st.session_state.get(BYOK_SESSION_KEY, "")
        return key or None
    except Exception:
        return None


def _claude_model() -> str:
    """Resolve the Claude model to use — ANTHROPIC_MODEL secret/env if set, else the built-in default."""
    try:
        import streamlit as st
        value = st.secrets.get("ANTHROPIC_MODEL", "")
        if value:
            return value
    except Exception:
        pass
    return os.environ.get("ANTHROPIC_MODEL") or CLAUDE_MODEL


# ──────────────────────────────────────────────────────────────
# Tier 2: Groq (free hosted, OpenAI-compatible) — reachable from
# Streamlit Cloud, unlike Ollama which only works when the app
# itself is running on the same machine as the Ollama server.
# ──────────────────────────────────────────────────────────────
_PLACEHOLDER_MARKERS = ("paste-", "your-key", "your-api-key", "xxxx", "changeme")


def _groq_api_key() -> Optional[str]:
    """
    Read FREE_LLM_API_KEY (matches secrets.toml), with GROQ_API_KEY as an
    alias. Treats obvious unfilled placeholders (e.g. "paste-groq-key-here")
    as not configured, so a template secrets file doesn't silently break.
    Returns None if a forced provider mode excludes Groq.
    """
    if _provider_mode() not in ("auto", "groq"):
        return None
    key = None
    try:
        import streamlit as st
        key = st.secrets.get("FREE_LLM_API_KEY", "") or st.secrets.get("GROQ_API_KEY", "")
    except Exception:
        pass
    if not key:
        key = os.environ.get("FREE_LLM_API_KEY") or os.environ.get("GROQ_API_KEY")
    if not key:
        return None
    key = key.strip()
    if not key or any(marker in key.lower() for marker in _PLACEHOLDER_MARKERS):
        return None
    return key


def _groq_base_url() -> str:
    try:
        import streamlit as st
        return st.secrets.get("FREE_LLM_BASE_URL", "") or DEFAULT_GROQ_BASE_URL
    except Exception:
        return os.environ.get("FREE_LLM_BASE_URL") or DEFAULT_GROQ_BASE_URL


def _groq_model() -> str:
    try:
        import streamlit as st
        return st.secrets.get("FREE_LLM_MODEL", "") or DEFAULT_GROQ_MODEL
    except Exception:
        return os.environ.get("FREE_LLM_MODEL") or DEFAULT_GROQ_MODEL


def _groq_chat(system: str, messages: List[Dict], max_tokens: int = 1200) -> str:
    """OpenAI-compatible chat completion call against Groq's free API."""
    api_key = _groq_api_key()
    if not api_key:
        raise RuntimeError("No Groq/free-tier API key configured")
    resp = requests.post(
        f"{_groq_base_url()}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": _groq_model(),
            "messages": [{"role": "system", "content": system}] + list(messages),
            "max_tokens": max_tokens,
        },
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return (data.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()


def groq_chat_only(
    prompt: str,
    system: str = "You are a helpful, precise data engineering assistant.",
    max_tokens: int = 1200,
) -> Optional[Dict[str, Any]]:
    """
    Try the free hosted Groq tier only. Returns None instantly — no
    network call — when no real key is configured, so callers can chain
    it safely between claude_chat_only and their existing Ollama/template
    fallback with zero behavior change when it's not set up.
    """
    if not _groq_api_key():
        return None
    try:
        text = _groq_chat(system, [{"role": "user", "content": prompt}], max_tokens)
        if not text:
            return None
        return {"text": text, "provider": "Groq (free hosted)", "model": _groq_model()}
    except Exception:
        return None


PROVIDER_MODE_OVERRIDE_KEY = "provider_mode_session_override"
VALID_PROVIDER_MODES = ("auto", "claude", "groq", "ollama", "rule_based")


def set_session_provider_mode(mode: str) -> None:
    """Force a specific tier for THIS browser session only — lets you
    verify Groq/Ollama actually work even while a Claude key is present,
    without touching secrets.toml or redeploying."""
    try:
        import streamlit as st
        mode = (mode or "auto").strip().lower()
        if mode == "auto":
            st.session_state.pop(PROVIDER_MODE_OVERRIDE_KEY, None)
        else:
            st.session_state[PROVIDER_MODE_OVERRIDE_KEY] = mode
    except Exception:
        pass


def _provider_mode() -> str:
    """
    Resolve which tier should be used, in priority order:
    1. A session override set via Settings (this browser session only).
    2. AI_PROVIDER_MODE in secrets/env (deployment-wide default).
    3. "auto" — best available tier, Claude > Groq > Ollama > rule-based.
    Invalid/unrecognized values fall back to "auto" rather than silently
    disabling the app.
    """
    try:
        import streamlit as st
        override = st.session_state.get(PROVIDER_MODE_OVERRIDE_KEY, "")
        if override:
            return override
    except Exception:
        pass
    mode = ""
    try:
        import streamlit as st
        mode = st.secrets.get("AI_PROVIDER_MODE", "")
    except Exception:
        pass
    if not mode:
        mode = os.environ.get("AI_PROVIDER_MODE", "")
    mode = (mode or "auto").strip().lower()
    return mode if mode in VALID_PROVIDER_MODES else "auto"


def _claude_api_key() -> Optional[str]:
    """
    Resolve the Anthropic key to use, in priority order:
    1. BYOK — a key the user pasted in Settings for this session only.
    2. Deployment secret (Streamlit secrets.toml on the host running the app).
    3. Environment variable (local development).
    Returns None if AI_PROVIDER_MODE (or a session override) has forced a
    different tier, even when a real key is present — that's what makes
    "force Groq" or "force Ollama" actually testable.
    """
    if _provider_mode() not in ("auto", "claude"):
        return None
    session_key = _session_claude_api_key()
    if session_key:
        return session_key
    # Streamlit secrets (set in share.streamlit.io → Settings → Secrets)
    try:
        import streamlit as st
        key = st.secrets.get("ANTHROPIC_API_KEY", "")
        if key:
            return key
    except Exception:
        pass
    # Environment variable (local development)
    return os.environ.get("ANTHROPIC_API_KEY") or None


def _ollama_url() -> str:
    try:
        import streamlit as st
        return (
            st.secrets.get("OLLAMA_BASE_URL", "")
            or st.secrets.get("OLLAMA_URL", "")
            or DEFAULT_OLLAMA_URL
        )
    except Exception:
        return (
            os.environ.get("OLLAMA_BASE_URL")
            or os.environ.get("OLLAMA_URL")
            or DEFAULT_OLLAMA_URL
        )


def _ollama_model() -> str:
    try:
        import streamlit as st
        return st.secrets.get("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL)
    except Exception:
        return os.environ.get("OLLAMA_MODEL", DEFAULT_OLLAMA_MODEL)


# ──────────────────────────────────────────────────────────────
# Tier detection
# ──────────────────────────────────────────────────────────────
def _ollama_available() -> bool:
    if _provider_mode() not in ("auto", "ollama"):
        return False
    try:
        r = requests.get(f"{_ollama_url()}/api/tags", timeout=2)
        return r.status_code == 200
    except Exception:
        return False


def llm_status() -> Dict[str, Any]:
    """
    Return which LLM tier is active and why.
    Call this to show users what's powering the AI features.
    """
    mode = _provider_mode()
    forced = mode if mode != "auto" else None
    if _claude_api_key():
        byok = session_api_key_active()
        return {
            "tier": 1,
            "provider": "Claude API (your key)" if byok else "Claude API",
            "model": _claude_model(),
            "available": True,
            "byok": byok,
            "forced_mode": forced,
            "note": (
                "Using the Anthropic key you pasted in Settings for this session only."
                if byok
                else "Anthropic Claude API — best quality, works on Streamlit Cloud."
            ),
        }
    if _groq_api_key():
        return {
            "tier": 2,
            "provider": "Groq (free hosted)",
            "model": _groq_model(),
            "available": True,
            "byok": False,
            "forced_mode": forced,
            "note": "Free hosted Groq API — works from Streamlit Cloud, no local server needed.",
        }
    if _ollama_available():
        return {
            "tier": 3,
            "provider": "Ollama (local)",
            "model": _ollama_model(),
            "available": True,
            "byok": False,
            "forced_mode": forced,
            "note": f"Local Ollama at {_ollama_url()} — free, private, offline-capable.",
        }
    return {
        "tier": 4,
        "provider": "Rule-based fallback",
        "model": None,
        "available": True,
        "forced_mode": forced,
        "byok": False,
        "note": (
            "No LLM configured. Using deterministic rule-based diagnosis. "
            "Add ANTHROPIC_API_KEY, or a free FREE_LLM_API_KEY (Groq), in Streamlit → Settings → Secrets for live AI."
        ),
    }


# ──────────────────────────────────────────────────────────────
# Tier 1: Claude API
# ──────────────────────────────────────────────────────────────
def _claude_chat(
    system: str,
    messages: List[Dict],
    max_tokens: int = 1200,
    tools: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    """Non-streaming Claude API call. Returns full response dict."""
    api_key = _claude_api_key()
    if not api_key:
        raise RuntimeError("No Claude API key")

    payload: Dict[str, Any] = {
        "model": _claude_model(),
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages,
    }
    if tools:
        payload["tools"] = tools

    resp = requests.post(
        ANTHROPIC_API_URL,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def _claude_stream(
    system: str,
    messages: List[Dict],
    max_tokens: int = 1200,
) -> Generator[str, None, None]:
    """Streaming Claude API call. Yields text tokens."""
    api_key = _claude_api_key()
    if not api_key:
        raise RuntimeError("No Claude API key")

    resp = requests.post(
        ANTHROPIC_API_URL,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": _claude_model(),
            "max_tokens": max_tokens,
            "system": system,
            "messages": messages,
            "stream": True,
        },
        timeout=STREAM_TIMEOUT,
        stream=True,
    )
    resp.raise_for_status()

    for raw in resp.iter_lines():
        if not raw:
            continue
        line = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        if not line.startswith("data: "):
            continue
        payload = line[6:]
        if payload.strip() == "[DONE]":
            break
        try:
            event = json.loads(payload)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "content_block_delta":
            delta = event.get("delta", {})
            if delta.get("type") == "text_delta":
                yield delta.get("text", "")


def _claude_text(data: Dict) -> str:
    return "".join(
        b.get("text", "")
        for b in data.get("content", [])
        if b.get("type") == "text"
    )


# ──────────────────────────────────────────────────────────────
# Tier 2: Ollama
# ──────────────────────────────────────────────────────────────
def _ollama_chat(system: str, messages: List[Dict], max_tokens: int = 1200) -> str:
    ollama_messages = [{"role": "system", "content": system}] + messages
    resp = requests.post(
        f"{_ollama_url()}/api/chat",
        json={
            "model": _ollama_model(),
            "messages": ollama_messages,
            "stream": False,
            "options": {"num_predict": max_tokens},
        },
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("message", {}).get("content", "") or data.get("response", "")


def _ollama_stream(system: str, messages: List[Dict]) -> Generator[str, None, None]:
    ollama_messages = [{"role": "system", "content": system}] + messages
    resp = requests.post(
        f"{_ollama_url()}/api/chat",
        json={"model": _ollama_model(), "messages": ollama_messages, "stream": True},
        timeout=STREAM_TIMEOUT,
        stream=True,
    )
    resp.raise_for_status()
    for raw in resp.iter_lines():
        if not raw:
            continue
        try:
            event = json.loads(raw)
            token = event.get("message", {}).get("content", "")
            if token:
                yield token
            if event.get("done"):
                break
        except json.JSONDecodeError:
            continue


# ──────────────────────────────────────────────────────────────
# Tier 3: Rule-based fallback answers
# ──────────────────────────────────────────────────────────────
_RULE_ANSWERS = [
    {
        "match": ["delta merge conflict", "concurrent write", "concurrentappendexception"],
        "answer": (
            "ROOT_CAUSE: Two writers modified the same Delta table partitions simultaneously — "
            "optimistic concurrency control rejected one write.\n"
            "CONFIDENCE: high\n"
            "FIXED_CODE:\n```python\n"
            "import time\n"
            "for attempt in range(3):\n"
            "    try:\n"
            "        target.alias('t').merge(source.alias('s'), condition)\\\n"
            "            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n"
            "        break\n"
            "    except Exception as e:\n"
            "        if attempt == 2: raise\n"
            "        time.sleep(2 ** attempt)\n"
            "```"
        ),
    },
    {
        "match": ["column", "not found", "analysisexception", "cannot resolve"],
        "answer": (
            "ROOT_CAUSE: Spark could not resolve a column name at analysis time — "
            "likely a rename upstream, a missing alias, or a schema mismatch.\n"
            "CONFIDENCE: medium\n"
            "FIXED_CODE:\n```python\n"
            "# Check actual columns before referencing them\n"
            "print(df.columns)\n"
            "# Fix: rename if source uses different casing\n"
            "df = df.withColumnRenamed('deviceId', 'device_id')\n"
            "```"
        ),
    },
    {
        "match": ["outofmemoryerror", "oom", "executor lost", "shuffle"],
        "answer": (
            "ROOT_CAUSE: Executor ran out of memory during a shuffle or join — "
            "likely skewed data or too-large a collect().\n"
            "CONFIDENCE: medium\n"
            "FIXED_CODE:\n```python\n"
            "spark.conf.set('spark.sql.shuffle.partitions', '200')\n"
            "from pyspark.sql.functions import broadcast\n"
            "df = df.join(broadcast(small_df), on='device_id')\n"
            "```"
        ),
    },
    {
        "match": ["timeout", "read timed out", "connection refused"],
        "answer": (
            "ROOT_CAUSE: Source read exceeded the configured timeout — "
            "slow upstream, cluster under-provisioned, or stalled task.\n"
            "CONFIDENCE: low\n"
            "FIXED_CODE:\n```python\n"
            "spark.conf.set('spark.sql.broadcastTimeout', '600')\n"
            "spark.conf.set('spark.network.timeout', '800s')\n"
            "```"
        ),
    },
    {
        "match": ["scd", "slowly changing", "effective_from", "effective_to", "is_current"],
        "answer": (
            "ROOT_CAUSE: SCD Type 2 merge logic needs adjustment.\n"
            "CONFIDENCE: medium\n"
            "FIXED_CODE:\n```python\n"
            "# Close current version\n"
            "target.alias('t').merge(\n"
            "    source.alias('s'),\n"
            "    't.entity_id = s.entity_id AND t.is_current = true'\n"
            ").whenMatchedUpdate(\n"
            "    condition='t.status <> s.status',\n"
            "    set={'is_current': 'false', 'effective_to': 's.event_time'}\n"
            ").execute()\n"
            "# Insert new current version\n"
            "source.withColumn('is_current', lit(True))\\\n"
            "    .withColumn('effective_from', col('event_time'))\\\n"
            "    .write.format('delta').mode('append').save(target_path)\n"
            "```"
        ),
    },
    {
        "match": ["dedup", "duplicate", "dropduplicates", "row_number"],
        "answer": (
            "ROOT_CAUSE: Duplicate rows detected — incremental load needs deduplication.\n"
            "CONFIDENCE: high\n"
            "FIXED_CODE:\n```python\n"
            "from pyspark.sql import Window\n"
            "from pyspark.sql import functions as F\n"
            "w = Window.partitionBy('device_id').orderBy(F.col('event_time').desc())\n"
            "deduped = df.withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn')\n"
            "```"
        ),
    },
]


def _rule_answer(prompt: str) -> str:
    text = prompt.lower()
    for rule in _RULE_ANSWERS:
        if any(kw in text for kw in rule["match"]):
            return rule["answer"]
    return (
        "ROOT_CAUSE: No matching pattern found. Manual investigation required.\n"
        "CONFIDENCE: low\n"
        "FIXED_CODE:\n```python\n# Review error message and stack trace manually\npass\n```"
    )


# ──────────────────────────────────────────────────────────────
# Public unified interface
# ──────────────────────────────────────────────────────────────
SYSTEM_PROMPT_AGENT = (
    "You are DataDoctor AI — an autonomous data-pipeline diagnostic agent for "
    "Azure Databricks, PySpark, Delta Lake, ADF, and Spark Structured Streaming. "
    "When given a failed step, diagnose the root cause and propose a fix. "
    "Always respond in this exact format:\n\n"
    "ROOT_CAUSE: <one or two sentences>\n"
    "CONFIDENCE: <low|medium|high>\n"
    "FIXED_CODE:\n```python\n<corrected code>\n```"
)

SYSTEM_PROMPT_CHAT = (
    "You are DataDoctor AI — a senior data engineering assistant specialising in "
    "Azure Databricks, PySpark, Delta Lake, ADF, and Spark Structured Streaming. "
    "Give specific, working code examples. Explain trade-offs clearly. Be concise."
)

SYSTEM_PROMPT_CODE = (
    "You are a PySpark and Delta Lake code assistant. "
    "Given a plain-language request, respond with a working code snippet plus one sentence explanation. "
    "Format:\nEXPLANATION: <one sentence>\n```python\n<code>\n```"
)


def claude_chat_only(
    prompt: str,
    system: str = SYSTEM_PROMPT_CHAT,
    max_tokens: int = 1200,
) -> Optional[Dict[str, Any]]:
    """
    Try Claude API ONLY (BYOK session key, else deployment secret).
    Returns None immediately — no network call — if no key is configured,
    so every caller's existing free-tier (Ollama / rule-based) path is
    completely unaffected when Claude isn't set up. This is what lets
    Code Assistant, Chat, and AI Agent repair diagnosis pick up a paid
    key automatically without changing their own fallback logic.
    """
    if not _claude_api_key():
        return None
    try:
        data = _claude_chat(system, [{"role": "user", "content": prompt}], max_tokens)
        text = _claude_text(data)
        if not text:
            return None
        return {
            "text": text,
            "provider": "Claude API (your key)" if session_api_key_active() else "Claude API",
            "model": _claude_model(),
            "byok": session_api_key_active(),
        }
    except Exception:
        return None


def llm_chat(
    prompt: str,
    system: str = SYSTEM_PROMPT_CHAT,
    history: Optional[List[Dict]] = None,
    max_tokens: int = 1200,
) -> Dict[str, Any]:
    """
    Send a chat message through the best available LLM tier.

    Returns:
        {
            "text": str,
            "provider": str,
            "tier": int,
            "error": str | None
        }
    """
    messages = list(history or []) + [{"role": "user", "content": prompt}]

    # Tier 1: Claude API
    if _claude_api_key():
        try:
            data = _claude_chat(system, messages, max_tokens)
            return {
                "text": _claude_text(data),
                "provider": "Claude API",
                "tier": 1,
                "error": None,
            }
        except Exception as e:
            pass  # fall through to tier 2

    # Tier 2: Groq (free hosted)
    if _groq_api_key():
        try:
            text = _groq_chat(system, messages, max_tokens)
            if text:
                return {
                    "text": text,
                    "provider": "Groq (free hosted)",
                    "tier": 2,
                    "error": None,
                }
        except Exception:
            pass  # fall through to tier 3

    # Tier 3: Ollama
    if _ollama_available():
        try:
            text = _ollama_chat(system, messages, max_tokens)
            return {
                "text": text,
                "provider": "Ollama (local)",
                "tier": 3,
                "error": None,
            }
        except Exception as e:
            pass

    # Tier 4: Rule-based
    return {
        "text": _rule_answer(prompt),
        "provider": "Rule-based fallback",
        "tier": 4,
        "error": None,
    }


def llm_stream(
    prompt: str,
    system: str = SYSTEM_PROMPT_CHAT,
    history: Optional[List[Dict]] = None,
) -> Generator[str, None, None]:
    """
    Stream tokens from the best available LLM tier.
    Yields string tokens. If streaming is unavailable, yields the full text at once.
    """
    messages = list(history or []) + [{"role": "user", "content": prompt}]

    # Tier 1: Claude API streaming
    if _claude_api_key():
        try:
            yield from _claude_stream(system, messages)
            return
        except Exception:
            pass

    # Tier 2: Ollama streaming
    if _ollama_available():
        try:
            yield from _ollama_stream(system, messages)
            return
        except Exception:
            pass

    # Tier 3: rule-based — yield full text at once (no streaming possible)
    yield _rule_answer(prompt)


def llm_chat_with_tools(
    prompt: str,
    system: str = SYSTEM_PROMPT_AGENT,
    tools: Optional[List[Dict]] = None,
    tool_runner=None,
) -> Dict[str, Any]:
    """
    Agentic tool-calling loop (Claude API only).
    Falls back to llm_chat if Claude is not available.
    """
    if not _claude_api_key():
        return llm_chat(prompt, system)

    messages = [{"role": "user", "content": prompt}]
    tools_used = []

    for _ in range(5):
        try:
            data = _claude_chat(system, messages, tools=tools or [])
        except Exception:
            return llm_chat(prompt, system)

        stop_reason = data.get("stop_reason")
        content = data.get("content", [])

        if stop_reason == "end_turn":
            return {
                "text": _claude_text(data),
                "provider": "Claude API (tool-augmented)",
                "tier": 1,
                "tools_used": tools_used,
                "error": None,
            }

        if stop_reason == "tool_use" and tool_runner:
            tool_results = []
            messages.append({"role": "assistant", "content": content})
            for block in content:
                if block.get("type") != "tool_use":
                    continue
                name = block["name"]
                inp = block.get("input", {})
                tools_used.append(name)
                result = tool_runner(name, inp)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block["id"],
                    "content": result,
                })
            messages.append({"role": "user", "content": tool_results})
        else:
            break

    return llm_chat(prompt, system)
