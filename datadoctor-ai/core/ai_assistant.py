"""
AI-powered PySpark/SQL code suggestions.

Uses the Anthropic API if an API key is configured (via st.secrets or the
ANTHROPIC_API_KEY env var). If no key is present, falls back to a small
local template library covering the most common data-engineering patterns
(incremental load, SCD2, dedup, MERGE upsert) — so the app is still fully
functional and demoable on a free deployment with zero API cost.
"""
import os
from dataclasses import dataclass

import requests

ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
MODEL = "claude-sonnet-4-6"

SYSTEM_PROMPT = (
    "You are a senior data engineer assistant embedded in a pipeline-authoring tool. "
    "Given a plain-language request, respond with a single idiomatic PySpark or SQL code "
    "snippet (whichever the user's engine selection specifies), no more than ~30 lines, "
    "plus a one-sentence explanation. Prefer Delta Lake idioms (MERGE, mergeSchema, "
    "Z-Ordering) where relevant. Output format:\n\n"
    "EXPLANATION: <one sentence>\n"
    "```<language>\n<code>\n```"
)

FALLBACK_TEMPLATES = {
    "incremental load": (
        "EXPLANATION: Comparison-based incremental load using a watermark column to isolate new/changed rows.\n"
        "```python\n"
        "latest_watermark = spark.read.table(\"pipeline_state\").agg({\"max_ts\": \"max\"}).collect()[0][0]\n"
        "incremental_df = (\n"
        "    spark.read.table(\"source_table\")\n"
        "    .filter(col(\"updated_at\") > latest_watermark)\n"
        ")\n"
        "```"
    ),
    "scd2": (
        "EXPLANATION: SCD Type 2 upsert closing the current row and inserting a new version on change.\n"
        "```python\n"
        "target.alias(\"t\").merge(\n"
        "    source.alias(\"s\"), \"t.entity_id = s.entity_id AND t.is_current = true\"\n"
        ").whenMatchedUpdate(\n"
        "    condition=\"t.status <> s.status\",\n"
        "    set={\"is_current\": \"false\", \"effective_to\": \"s.event_time\"},\n"
        ").execute()\n"
        "```"
    ),
    "dedup": (
        "EXPLANATION: Deduplicate on a natural key, keeping the most recent record by event time.\n"
        "```python\n"
        "from pyspark.sql import Window\n"
        "from pyspark.sql import functions as F\n\n"
        "w = Window.partitionBy(\"device_id\").orderBy(F.col(\"event_time\").desc())\n"
        "deduped_df = df.withColumn(\"rn\", F.row_number().over(w)).filter(\"rn = 1\").drop(\"rn\")\n"
        "```"
    ),
    "merge upsert": (
        "EXPLANATION: Delta MERGE upsert keyed on a natural key.\n"
        "```python\n"
        "target.alias(\"t\").merge(\n"
        "    source_df.alias(\"s\"), \"t.device_id = s.device_id AND t.event_date = s.event_date\"\n"
        ").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()\n"
        "```"
    ),
    "default": (
        "EXPLANATION: General Spark SQL aggregation template — adjust grouping/metric columns for your case.\n"
        "```sql\n"
        "SELECT device_id, date_trunc('day', event_time) AS event_day, COUNT(*) AS event_count\n"
        "FROM events\n"
        "GROUP BY device_id, date_trunc('day', event_time)\n"
        "ORDER BY event_day DESC\n"
        "```"
    ),
}


@dataclass
class Suggestion:
    explanation: str
    code: str
    source: str  # "claude-api" or "local-template"


def _get_api_key() -> str | None:
    try:
        import streamlit as st

        if "ANTHROPIC_API_KEY" in st.secrets:
            return st.secrets["ANTHROPIC_API_KEY"]
    except Exception:
        pass
    return os.environ.get("ANTHROPIC_API_KEY")


def _parse_response_text(text: str) -> Suggestion:
    explanation = ""
    code = text
    if "EXPLANATION:" in text:
        after = text.split("EXPLANATION:", 1)[1]
        if "```" in after:
            explanation, rest = after.split("```", 1)
            code = "```" + rest
        else:
            explanation = after
    return Suggestion(explanation=explanation.strip(), code=code.strip(), source="claude-api")


def _fallback_suggestion(prompt: str) -> Suggestion:
    prompt_lower = prompt.lower()
    for key, template in FALLBACK_TEMPLATES.items():
        if key != "default" and key in prompt_lower:
            explanation, code = template.split("\n", 1)
            return Suggestion(
                explanation=explanation.replace("EXPLANATION: ", ""),
                code=code.strip(),
                source="local-template",
            )
    explanation, code = FALLBACK_TEMPLATES["default"].split("\n", 1)
    return Suggestion(
        explanation=explanation.replace("EXPLANATION: ", ""), code=code.strip(), source="local-template"
    )


def get_code_suggestion(prompt: str, engine: str = "pyspark") -> Suggestion:
    api_key = _get_api_key()
    if not api_key:
        return _fallback_suggestion(prompt)

    full_prompt = f"Engine: {engine}\nRequest: {prompt}"
    try:
        response = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": MODEL,
                "max_tokens": 600,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": full_prompt}],
            },
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        text = "".join(block.get("text", "") for block in data.get("content", []))
        return _parse_response_text(text)
    except Exception:
        # network/auth failure -> degrade gracefully rather than break the UI
        fallback = _fallback_suggestion(prompt)
        fallback.source = "local-template-fallback-after-api-error"
        return fallback
