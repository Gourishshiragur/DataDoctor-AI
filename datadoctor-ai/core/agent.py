"""
Agent layer — full agentic loop with RAG, tool calling, and streaming.

Skills demonstrated:
  RAG          : retrieve_similar() pulls semantically-matched past incidents
                 before the LLM reasons, so the agent has "memory" of prior fixes
  Tool calling : the agent is given a tool registry; Claude decides which tools
                 to invoke (search_incidents, explain_error, suggest_optimization)
                 and the runner executes them, feeding results back
  Streaming    : LLM tokens are yielded in real time via a generator so the UI
                 can display them as they arrive rather than waiting for the full response
  Agentic loop : observe (gather context + RAG) → reason (tool calls + LLM) → act
                 (apply fix + store to RAG memory so future agents learn from this)

All features degrade gracefully: no API key → rule-based diagnoses + fallback vector
store. The app never crashes due to a missing dependency.
"""
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generator, List, Optional

import requests

from core import store
from core.ai_assistant import _get_api_key, ANTHROPIC_API_URL, MODEL
from core.rag_memory import Incident, MemoryResult, incident_count
from core.rag_memory import retrieve_similar, store_incident


# ─────────────────────────────────────────────
# Tool registry
# ─────────────────────────────────────────────
TOOLS = [
    {
        "name": "search_past_incidents",
        "description": (
            "Search the RAG memory store for past pipeline incidents that are semantically "
            "similar to the current error. Returns up to 3 resolved incidents with their "
            "root cause and the fix that was applied. Use this first before reasoning from scratch."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Natural-language description of the error or symptom to search for.",
                }
            },
            "required": ["query"],
        },
    },
    {
        "name": "explain_error",
        "description": "Return a plain-English explanation of a PySpark or Delta Lake exception class name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "exception_class": {
                    "type": "string",
                    "description": "The exception class name, e.g. 'AnalysisException', 'OutOfMemoryError'.",
                }
            },
            "required": ["exception_class"],
        },
    },
    {
        "name": "suggest_optimization",
        "description": (
            "Given a PySpark code snippet, return concrete performance-tuning suggestions "
            "(partition pruning, broadcast joins, Z-Ordering, caching) relevant to the code."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "The PySpark code to analyze."}
            },
            "required": ["code"],
        },
    },
]

AGENT_SYSTEM_PROMPT = """You are DataDoctor AI — an autonomous data-pipeline diagnostic agent \
embedded in a Lakehouse ops console.

You have access to tools. Always call search_past_incidents FIRST using the error message as \
the query — past incidents may already contain the exact fix. Only call explain_error if the \
exception class is unfamiliar. Call suggest_optimization if the fix involves performance tuning.

After tool results are available, produce your final diagnosis in exactly this format:

ROOT_CAUSE: <one or two sentences>
CONFIDENCE: <low|medium|high>
FIXED_CODE:
```
<corrected code>
```
"""

ERROR_EXPLANATIONS = {
    "AnalysisException": "Spark could not resolve a column, function, or table name at analysis time — usually a schema mismatch, missing alias, or wrong column name.",
    "OutOfMemoryError": "A JVM executor ran out of heap memory — typically during a large shuffle, skewed join, or when too much data is collected to the driver.",
    "DeltaConcurrentModificationException": "Two writers tried to modify the same Delta table partition simultaneously — optimistic concurrency control rejected one write.",
    "TimeoutException": "A network call or Spark operation exceeded the configured time limit — can indicate a slow source, cluster under-provisioning, or a stalled task.",
    "StreamingQueryException": "The Spark Structured Streaming query encountered an unrecoverable error — check the cause field for the underlying exception.",
    "SchemaEvolutionException": "The incoming data schema is incompatible with the existing Delta table schema in a way that cannot be auto-merged — requires schema migration.",
}

RULE_BASED_DIAGNOSES = [
    {"match": "column 'device_id' not found",
     "root_cause": "The upstream schema doesn't contain 'device_id' at this stage — likely a rename or select upstream.",
     "confidence": "medium",
     "fix_hint": "df = df.withColumnRenamed('deviceId', 'device_id')\nassert 'device_id' in df.columns"},
    {"match": "Delta MERGE conflict",
     "root_cause": "A concurrent writer touched the same Delta table partitions during this MERGE, causing an optimistic concurrency failure.",
     "confidence": "high",
     "fix_hint": "for attempt in range(3):\n    try:\n        target.alias('t').merge(source.alias('s'), condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n        break\n    except Exception:\n        if attempt == 2: raise\n        import time; time.sleep(2 ** attempt)"},
    {"match": "OutOfMemoryError",
     "root_cause": "Executor OOM during shuffle — likely skewed data or too-large a collect() call.",
     "confidence": "medium",
     "fix_hint": "spark.conf.set('spark.sql.shuffle.partitions', '200')\ndf = df.join(broadcast(small_df), on='device_id')"},
    {"match": "TimeoutException",
     "root_cause": "Source read exceeded the configured timeout.",
     "confidence": "low",
     "fix_hint": "spark.conf.set('spark.sql.broadcastTimeout', '600')"},
]


@dataclass
class Diagnosis:
    root_cause: str
    confidence: str
    fixed_code: str
    source: str
    similar_incidents: list
    tools_used: list


# ─────────────────────────────────────────────
# Tool execution
# ─────────────────────────────────────────────
def _run_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "search_past_incidents":
        results: List[MemoryResult] = retrieve_similar(tool_input["query"], k=3)
        if not results:
            return "No similar past incidents found in memory."
        lines = [f"Found {len(results)} similar past incident(s):\n"]
        for i, r in enumerate(results, 1):
            lines.append(
                f"Incident {i} (similarity: {r.similarity_score:.2f}, source: {r.source}):\n"
                f"  Error pattern: {r.incident.error_message}\n"
                f"  Root cause: {r.incident.root_cause}\n"
                f"  Fix applied: {r.incident.fix_applied}\n"
            )
        return "\n".join(lines)

    if tool_name == "explain_error":
        cls = tool_input.get("exception_class", "")
        for key, explanation in ERROR_EXPLANATIONS.items():
            if key.lower() in cls.lower():
                return f"{key}: {explanation}"
        return f"No explanation found for '{cls}'. Check Spark/Delta documentation."

    if tool_name == "suggest_optimization":
        code = tool_input.get("code", "")
        suggestions = []
        if "join" in code.lower() and "broadcast" not in code.lower():
            suggestions.append("Consider broadcast join if one side of the join is small: from pyspark.sql.functions import broadcast")
        if ".collect()" in code:
            suggestions.append("Avoid .collect() on large DataFrames — it pulls all data to the driver. Use .show(), .take(), or write to Delta instead.")
        if "groupBy" in code or "groupby" in code:
            suggestions.append("After groupBy/agg, set spark.conf.set('spark.sql.shuffle.partitions', '50') for mid-size data to avoid 200 tiny tasks.")
        if "write" in code and "partitionBy" not in code:
            suggestions.append("Add .partitionBy('date') or another high-cardinality column before .save() to enable partition pruning on reads.")
        if not suggestions:
            suggestions.append("No obvious performance issues found. Ensure Z-Ordering on frequently filtered columns: OPTIMIZE table ZORDER BY (device_id)")
        return "\n".join(f"• {s}" for s in suggestions)

    return f"Unknown tool: {tool_name}"


# ─────────────────────────────────────────────
# Context gathering
# ─────────────────────────────────────────────
def _gather_context(run: dict, pipeline: dict, step_id: str) -> dict:
    step = next(s for s in pipeline["steps"] if s["id"] == step_id)
    step_run = next(sr for sr in run["step_runs"] if sr["step_id"] == step_id)
    logs = [l for l in store.load_logs(run["id"]) if l["step_id"] == step_id]
    return {
        "step_name": step["name"],
        "code": step["code"],
        "engine": step["engine"],
        "error_message": step_run.get("error_message") or "",
        "recent_logs": [l["message"] for l in logs[-10:]],
        "retry_count": step_run.get("retry_count", 0),
    }


# ─────────────────────────────────────────────
# Rule-based fallback
# ─────────────────────────────────────────────
def _rule_based_diagnosis(context: dict, similar: list) -> Diagnosis:
    error = context["error_message"]
    for rule in RULE_BASED_DIAGNOSES:
        if rule["match"].lower() in error.lower():
            return Diagnosis(
                root_cause=rule["root_cause"],
                confidence=rule["confidence"],
                fixed_code=context["code"].rstrip() + "\n\n# --- agent-suggested fix ---\n" + rule["fix_hint"],
                source="rule-based",
                similar_incidents=similar,
                tools_used=["search_past_incidents"],
            )
    return Diagnosis(
        root_cause=f"No known pattern matched: '{error}'. Manual investigation needed.",
        confidence="low",
        fixed_code=context["code"],
        source="rule-based",
        similar_incidents=similar,
        tools_used=[],
    )


# ─────────────────────────────────────────────
# LLM agentic loop with tool calling + streaming
# ─────────────────────────────────────────────
def _parse_diagnosis(text: str, original_code: str, similar: list, tools_used: list) -> Diagnosis:
    root_cause_match = re.search(r"ROOT_CAUSE:\s*(.+)", text)
    confidence_match = re.search(r"CONFIDENCE:\s*(\w+)", text)
    code_match = re.search(r"```(?:\w+)?\n(.*?)```", text, re.DOTALL)
    return Diagnosis(
        root_cause=root_cause_match.group(1).strip() if root_cause_match else "See full response.",
        confidence=confidence_match.group(1).strip().lower() if confidence_match else "low",
        fixed_code=code_match.group(1).strip() if code_match else original_code,
        source="claude-api",
        similar_incidents=similar,
        tools_used=tools_used,
    )


def diagnose(run: dict, pipeline: dict, step_id: str) -> Diagnosis:
    """
    Full agentic diagnosis: RAG retrieval → tool-augmented LLM reasoning → structured output.
    Non-streaming version (for compatibility). See diagnose_stream() for streaming.
    """
    context = _gather_context(run, pipeline, step_id)

    # Always retrieve similar incidents first (RAG step — happens even without API key)
    similar = retrieve_similar(
        f"{context['error_message']} {context['code'][:200]}", k=3
    )

    api_key = _get_api_key()
    if not api_key:
        return _rule_based_diagnosis(context, similar)

    # Build initial user message with full context
    similar_text = ""
    if similar:
        parts = []
        for r in similar:
            parts.append(
                f"  Past incident (similarity {r.similarity_score:.2f}):\n"
                f"    Error: {r.incident.error_message}\n"
                f"    Root cause: {r.incident.root_cause}\n"
                f"    Fix: {r.incident.fix_applied}"
            )
        similar_text = "\nRAG MEMORY — similar past incidents:\n" + "\n".join(parts) + "\n"

    user_message = (
        f"Pipeline: {pipeline.get('name', 'unknown')}\n"
        f"Step: {context['step_name']} ({context['engine']})\n"
        f"Error: {context['error_message']}\n"
        f"Retry count: {context['retry_count']}\n"
        f"{similar_text}\n"
        f"Code:\n{context['code']}\n\n"
        f"Recent logs:\n" + "\n".join(context["recent_logs"])
    )

    messages = [{"role": "user", "content": user_message}]
    tools_used = []

    # Agentic tool-calling loop: run until the model produces a final text response
    for _ in range(5):  # safety cap on tool rounds
        try:
            resp = requests.post(
                ANTHROPIC_API_URL,
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": MODEL,
                    "max_tokens": 1200,
                    "system": AGENT_SYSTEM_PROMPT,
                    "tools": TOOLS,
                    "messages": messages,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return _rule_based_diagnosis(context, similar)

        stop_reason = data.get("stop_reason")
        content = data.get("content", [])

        if stop_reason == "end_turn":
            text = "".join(b.get("text", "") for b in content if b.get("type") == "text")
            return _parse_diagnosis(text, context["code"], similar, tools_used)

        if stop_reason == "tool_use":
            # Execute every tool the model requested
            tool_results = []
            messages.append({"role": "assistant", "content": content})
            for block in content:
                if block.get("type") != "tool_use":
                    continue
                tool_name = block["name"]
                tool_input = block.get("input", {})
                tools_used.append(tool_name)
                result = _run_tool(tool_name, tool_input)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block["id"],
                    "content": result,
                })
            messages.append({"role": "user", "content": tool_results})
            continue

        break

    return _rule_based_diagnosis(context, similar)


def diagnose_stream(run: dict, pipeline: dict, step_id: str) -> Generator[str, None, Diagnosis]:
    """
    Streaming version: yields token strings as they arrive so the UI can display
    them progressively. Yields a sentinel dict as the last item containing the
    full Diagnosis object once the stream is complete.

    Usage in Streamlit:
        for chunk in diagnose_stream(run, pipeline, step_id):
            if isinstance(chunk, dict):  # final sentinel
                diagnosis = chunk["diagnosis"]
            else:
                display_token(chunk)
    """
    context = _gather_context(run, pipeline, step_id)
    similar = retrieve_similar(f"{context['error_message']} {context['code'][:200]}", k=3)
    api_key = _get_api_key()

    if not api_key:
        diagnosis = _rule_based_diagnosis(context, similar)
        yield diagnosis.root_cause
        yield {"diagnosis": diagnosis}
        return

    similar_text = ""
    if similar:
        parts = [
            f"  Past incident (similarity {r.similarity_score:.2f}):\n"
            f"    Error: {r.incident.error_message}\n    Fix: {r.incident.fix_applied}"
            for r in similar
        ]
        similar_text = "\nRAG MEMORY:\n" + "\n".join(parts) + "\n"

    user_message = (
        f"Step: {context['step_name']} ({context['engine']})\n"
        f"Error: {context['error_message']}\n"
        f"{similar_text}\nCode:\n{context['code']}\n\n"
        f"Logs:\n" + "\n".join(context["recent_logs"])
    )

    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": 1200,
                "system": AGENT_SYSTEM_PROMPT,
                "tools": TOOLS,
                "stream": True,
                "messages": [{"role": "user", "content": user_message}],
            },
            timeout=60,
            stream=True,
        )
        resp.raise_for_status()
    except Exception:
        diagnosis = _rule_based_diagnosis(context, similar)
        yield diagnosis.root_cause
        yield {"diagnosis": diagnosis}
        return

    full_text = ""
    for raw_line in resp.iter_lines():
        if not raw_line:
            continue
        line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
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
                token = delta.get("text", "")
                full_text += token
                yield token

    diagnosis = _parse_diagnosis(full_text, context["code"], similar, ["streaming"])
    yield {"diagnosis": diagnosis}


# ─────────────────────────────────────────────
# ACT: apply fix + store to RAG memory
# ─────────────────────────────────────────────
def apply_fix(pipeline: dict, step_id: str, fixed_code: str) -> dict:
    """Patch the step's code in the pipeline definition."""
    for step in pipeline["steps"]:
        if step["id"] == step_id:
            step["code"] = fixed_code
    store.save_pipeline(pipeline)
    return pipeline


def record_resolved_incident(
    run: dict, pipeline: dict, step_id: str, diagnosis: Diagnosis
):
    """
    Store a resolved incident to the RAG memory so future agent calls can
    retrieve it as context. This is what makes the system learn over time.
    """
    step = next((s for s in pipeline["steps"] if s["id"] == step_id), {})
    step_run = next((sr for sr in run["step_runs"] if sr["step_id"] == step_id), {})
    incident = Incident(
        id=f"inc-{step_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        error_message=step_run.get("error_message") or "",
        step_code=step.get("code", "")[:500],
        root_cause=diagnosis.root_cause,
        fix_applied=diagnosis.fixed_code[:500],
        pipeline_name=pipeline.get("name", ""),
        resolved_at=datetime.now(timezone.utc).isoformat(),
        confidence=diagnosis.confidence,
    )
    store_incident(incident)
