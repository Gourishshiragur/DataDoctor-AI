"""
DataDoctor AI — Enterprise Agent Layer

Agentic workflow:

OBSERVE
- Read pipeline, failed step, logs, retry information, and code.
- Retrieve semantically similar resolved incidents from RAG memory.

REASON
- Use the free local Ollama provider when available.
- Ground the model with pipeline context and incident memory.
- Fall back to deterministic diagnostic rules when Ollama is unavailable.

ACT
- Generate corrected code.
- Apply approved fixes to pipeline definitions.
- Store resolved incidents in RAG memory so future diagnoses can reuse them.

Design goals:
- No paid API required
- Free local Ollama support
- RAG incident memory
- Graceful offline fallback
- Existing agent-page compatibility
- Existing apply-fix workflow preserved
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Generator, List

from core import store

from core.ai_assistant import (
    DEFAULT_OLLAMA_MODEL,
    DEFAULT_OLLAMA_URL,
    ask_data_doctor,
)

from core.rag_memory import (
    Incident,
    MemoryResult,
    retrieve_similar,
    store_incident,
)


# ============================================================
# Agent tool registry
# ============================================================

TOOLS = [
    {
        "name": "search_past_incidents",
        "description": (
            "Search incident RAG memory for semantically "
            "similar resolved pipeline failures."
        ),
    },
    {
        "name": "explain_error",
        "description": (
            "Explain common Spark, PySpark, Delta Lake, "
            "streaming, schema, and infrastructure errors."
        ),
    },
    {
        "name": "suggest_optimization",
        "description": (
            "Inspect PySpark code and return relevant "
            "performance recommendations."
        ),
    },
]


AGENT_SYSTEM_PROMPT = """
You are DataDoctor AI, an enterprise data-pipeline diagnostic
and remediation agent.

Analyze only the supplied pipeline evidence.

Use:
- Failed-step error
- Pipeline metadata
- Step code
- Retry information
- Recent logs
- Retrieved incident-memory context

Do not invent:
- Tables
- Columns
- Pipeline executions
- Business values
- Metrics
- Root causes unsupported by evidence

Return exactly:

ROOT_CAUSE: one or two evidence-based sentences

CONFIDENCE: low, medium, or high

FIXED_CODE:
corrected complete code

BUSINESS_IMPACT:
short operational impact

VALIDATION:
specific checks required after applying the fix
""".strip()


# ============================================================
# Known error explanations
# ============================================================

ERROR_EXPLANATIONS = {
    "AnalysisException": (
        "Spark could not resolve a column, function, table, "
        "or expression during logical-plan analysis. Common "
        "causes include schema mismatch, missing aliases, "
        "incorrect column names, and ambiguous references."
    ),
    "OutOfMemoryError": (
        "A JVM executor or driver exhausted available heap "
        "memory. Common causes include large shuffles, data "
        "skew, oversized partitions, collect operations, or "
        "insufficient executor resources."
    ),
    "DeltaConcurrentModificationException": (
        "Concurrent operations modified overlapping Delta "
        "Lake files or partitions. Optimistic concurrency "
        "control rejected one transaction."
    ),
    "ConcurrentModificationException": (
        "Multiple operations attempted to modify the same "
        "data or metadata concurrently."
    ),
    "TimeoutException": (
        "A source request, Spark operation, network call, or "
        "broadcast operation exceeded its configured timeout."
    ),
    "StreamingQueryException": (
        "A Structured Streaming query encountered an "
        "unrecoverable error. The nested cause and checkpoint "
        "state should be inspected."
    ),
    "SchemaEvolutionException": (
        "Incoming data is incompatible with the existing "
        "target schema and cannot be merged automatically."
    ),
    "FileNotFoundException": (
        "An expected source, checkpoint, configuration, or "
        "data file could not be found."
    ),
    "Py4JJavaError": (
        "A JVM-side Spark exception was propagated through "
        "the Python-to-Java Py4J bridge. The nested Java "
        "exception contains the actual cause."
    ),
}


# ============================================================
# Deterministic diagnostic rules
# ============================================================

RULE_BASED_DIAGNOSES = [
    {
        "patterns": [
            "column 'device_id' not found",
            "cannot resolve 'device_id'",
            "cannot resolve `device_id`",
            "unresolved column",
        ],
        "root_cause": (
            "The failed transformation references a column "
            "that is unavailable at this stage. The source "
            "schema may use another name, or an earlier "
            "select, join, aggregation, or rename operation "
            "may have removed the required column."
        ),
        "confidence": "medium",
        "fix_hint": (
            "# Validate the source schema before using "
            "device_id\n"
            "required_columns = ['device_id']\n"
            "missing_columns = [\n"
            "    column\n"
            "    for column in required_columns\n"
            "    if column not in df.columns\n"
            "]\n\n"
            "if missing_columns:\n"
            "    raise ValueError(\n"
            "        f'Missing required columns: "
            "{missing_columns}. '\n"
            "        f'Available columns: {df.columns}'\n"
            "    )"
        ),
    },
    {
        "patterns": [
            "delta merge conflict",
            "deltaconcurrentmodificationexception",
            "concurrent modification",
            "concurrentmodificationexception",
        ],
        "root_cause": (
            "Another writer modified overlapping Delta files "
            "or partitions while the MERGE operation was "
            "running. Delta optimistic concurrency control "
            "rejected the conflicting transaction."
        ),
        "confidence": "high",
        "fix_hint": (
            "import time\n\n"
            "maximum_attempts = 3\n\n"
            "for attempt in range(maximum_attempts):\n"
            "    try:\n"
            "        (\n"
            "            target.alias('target')\n"
            "            .merge(\n"
            "                source.alias('source'),\n"
            "                merge_condition,\n"
            "            )\n"
            "            .whenMatchedUpdateAll()\n"
            "            .whenNotMatchedInsertAll()\n"
            "            .execute()\n"
            "        )\n"
            "        break\n\n"
            "    except Exception:\n"
            "        if attempt == maximum_attempts - 1:\n"
            "            raise\n\n"
            "        time.sleep(2 ** attempt)"
        ),
    },
    {
        "patterns": [
            "outofmemoryerror",
            "java heap space",
            "gc overhead limit exceeded",
        ],
        "root_cause": (
            "Spark exhausted available JVM memory during a "
            "memory-intensive operation. Large shuffle "
            "partitions, data skew, oversized joins, caching, "
            "or driver-side collection may be contributing."
        ),
        "confidence": "medium",
        "fix_hint": (
            "# Review partition size and skew before changing "
            "cluster resources.\n"
            "spark.conf.set(\n"
            "    'spark.sql.adaptive.enabled',\n"
            "    'true',\n"
            ")\n\n"
            "spark.conf.set(\n"
            "    'spark.sql.adaptive.skewJoin.enabled',\n"
            "    'true',\n"
            ")\n\n"
            "# Repartition using an appropriate business or "
            "join key.\n"
            "optimized_df = df.repartition('device_id')"
        ),
    },
    {
        "patterns": [
            "timeoutexception",
            "timed out",
            "timeout",
        ],
        "root_cause": (
            "The operation exceeded its configured time "
            "limit. The cause may be a slow source, network "
            "latency, an under-provisioned cluster, a stalled "
            "task, or an unexpectedly large broadcast."
        ),
        "confidence": "low",
        "fix_hint": (
            "# Increase this only after confirming that a "
            "broadcast timeout is the cause.\n"
            "spark.conf.set(\n"
            "    'spark.sql.broadcastTimeout',\n"
            "    '600',\n"
            ")"
        ),
    },
    {
        "patterns": [
            "streamingqueryexception",
            "checkpoint",
        ],
        "root_cause": (
            "The Structured Streaming query failed because "
            "of an underlying processing, schema, source, "
            "sink, or checkpoint issue. The nested exception "
            "and checkpoint compatibility require review."
        ),
        "confidence": "medium",
        "fix_hint": (
            "# Preserve a stable, unique checkpoint path for "
            "this stream.\n"
            "query = (\n"
            "    streaming_df\n"
            "    .writeStream\n"
            "    .format('delta')\n"
            "    .option(\n"
            "        'checkpointLocation',\n"
            "        checkpoint_path,\n"
            "    )\n"
            "    .start(target_path)\n"
            ")"
        ),
    },
]


# ============================================================
# Structured diagnosis result
# ============================================================

@dataclass
class Diagnosis:
    root_cause: str
    confidence: str
    fixed_code: str
    source: str
    similar_incidents: list
    tools_used: list
    business_impact: str = ""
    validation: str = ""


# ============================================================
# Tool execution
# ============================================================

def _run_tool(
    tool_name: str,
    tool_input: dict,
) -> str:
    """
    Execute deterministic local agent tools.
    """

    if tool_name == "search_past_incidents":
        query = str(
            tool_input.get(
                "query",
                "",
            )
        )

        results: List[MemoryResult] = (
            retrieve_similar(
                query,
                k=3,
            )
        )

        if not results:
            return (
                "No similar resolved incidents were found "
                "in RAG memory."
            )

        lines = [
            (
                f"Found {len(results)} similar resolved "
                "incident(s)."
            )
        ]

        for index, result in enumerate(
            results,
            start=1,
        ):
            lines.append(
                (
                    f"\nIncident {index}\n"
                    f"Similarity: "
                    f"{result.similarity_score:.2f}\n"
                    f"Backend: {result.source}\n"
                    f"Error: "
                    f"{result.incident.error_message}\n"
                    f"Root cause: "
                    f"{result.incident.root_cause}\n"
                    f"Applied fix: "
                    f"{result.incident.fix_applied}"
                )
            )

        return "\n".join(
            lines
        )

    if tool_name == "explain_error":
        exception_class = str(
            tool_input.get(
                "exception_class",
                "",
            )
        )

        for name, explanation in (
            ERROR_EXPLANATIONS.items()
        ):
            if (
                name.lower()
                in exception_class.lower()
            ):
                return (
                    f"{name}: {explanation}"
                )

        return (
            "No built-in explanation was found for "
            f"'{exception_class}'. Inspect the nested Spark "
            "or JVM exception for the underlying cause."
        )

    if tool_name == "suggest_optimization":
        code = str(
            tool_input.get(
                "code",
                "",
            )
        )

        code_lower = (
            code.lower()
        )

        suggestions = []

        if (
            "join" in code_lower
            and "broadcast" not in code_lower
        ):
            suggestions.append(
                "Consider a broadcast join only when one "
                "side is verified to be small."
            )

        if ".collect()" in code_lower:
            suggestions.append(
                "Avoid collect() for large DataFrames "
                "because it transfers all records to the "
                "driver."
            )

        if (
            "groupby" in code_lower
            or "groupby(" in code_lower
        ):
            suggestions.append(
                "Inspect shuffle partition sizes and data "
                "skew after aggregation."
            )

        if (
            ".write" in code_lower
            and "partitionby" not in code_lower
        ):
            suggestions.append(
                "Evaluate target partitioning using common "
                "filter columns with controlled cardinality."
            )

        if (
            ".cache()" in code_lower
            or ".persist(" in code_lower
        ):
            suggestions.append(
                "Cache only reused DataFrames and unpersist "
                "them after their final action."
            )

        if not suggestions:
            suggestions.append(
                "No obvious code-level optimization issue "
                "was detected. Inspect the physical plan, "
                "shuffle size, partition distribution, file "
                "sizes, and data skew."
            )

        return "\n".join(
            f"- {suggestion}"
            for suggestion
            in suggestions
        )

    return (
        f"Unknown agent tool: "
        f"{tool_name}"
    )


# ============================================================
# Safe context gathering
# ============================================================

def _gather_context(
    run: dict,
    pipeline: dict,
    step_id: str,
) -> dict:
    """
    Gather failed-step evidence without assuming that every
    field exists.
    """

    pipeline_steps = (
        pipeline.get(
            "steps",
            [],
        )
        if isinstance(
            pipeline,
            dict,
        )
        else []
    )

    run_steps = (
        run.get(
            "step_runs",
            [],
        )
        if isinstance(
            run,
            dict,
        )
        else []
    )

    step = next(
        (
            candidate
            for candidate
            in pipeline_steps
            if isinstance(
                candidate,
                dict,
            )
            and candidate.get(
                "id"
            )
            == step_id
        ),
        {},
    )

    step_run = next(
        (
            candidate
            for candidate
            in run_steps
            if isinstance(
                candidate,
                dict,
            )
            and candidate.get(
                "step_id"
            )
            == step_id
        ),
        {},
    )

    run_id = (
        run.get(
            "id",
            "",
        )
        if isinstance(
            run,
            dict,
        )
        else ""
    )

    logs = []

    try:
        logs = (
            store.load_logs(
                run_id
            )
            if run_id
            else []
        )

    except Exception:
        logs = []

    matching_logs = []

    for log in logs:
        if not isinstance(
            log,
            dict,
        ):
            continue

        log_step_id = (
            log.get(
                "step_id"
            )
        )

        if (
            log_step_id
            and log_step_id
            != step_id
        ):
            continue

        message = (
            log.get(
                "message"
            )
            or log.get(
                "log_message"
            )
            or log.get(
                "error"
            )
        )

        if message:
            matching_logs.append(
                str(
                    message
                )
            )

    return {
        "pipeline_name": (
            pipeline.get(
                "name",
                "Unknown pipeline",
            )
            if isinstance(
                pipeline,
                dict,
            )
            else "Unknown pipeline"
        ),
        "step_id": step_id,
        "step_name": (
            step.get(
                "name"
            )
            or step.get(
                "step_name"
            )
            or step_id
        ),
        "code": str(
            step.get(
                "code",
                "",
            )
        ),
        "engine": str(
            step.get(
                "engine",
                "Unknown",
            )
        ),
        "error_message": str(
            step_run.get(
                "error_message"
            )
            or step_run.get(
                "error"
            )
            or ""
        ),
        "recent_logs": (
            matching_logs[-10:]
        ),
        "retry_count": (
            step_run.get(
                "retry_count",
                0,
            )
        ),
        "run_status": (
            run.get(
                "status",
                "UNKNOWN",
            )
            if isinstance(
                run,
                dict,
            )
            else "UNKNOWN"
        ),
    }


# ============================================================
# RAG formatting
# ============================================================

def _retrieve_incident_memory(
    context: dict,
    k: int = 3,
) -> List[MemoryResult]:
    """
    Retrieve similar incidents using error and code context.
    """

    query = (
        f"{context.get('error_message', '')}\n"
        f"{context.get('step_name', '')}\n"
        f"{context.get('engine', '')}\n"
        f"{context.get('code', '')[:500]}"
    )

    try:
        return retrieve_similar(
            query,
            k=k,
        )

    except Exception:
        return []


def _format_incident_memory(
    similar: List[MemoryResult],
) -> str:
    """
    Convert retrieved incidents into grounded LLM context.
    """

    if not similar:
        return (
            "No similar resolved incidents were retrieved."
        )

    parts = []

    for index, result in enumerate(
        similar,
        start=1,
    ):
        parts.append(
            (
                f"Resolved incident {index}\n"
                f"Similarity: "
                f"{result.similarity_score:.2f}\n"
                f"Error: "
                f"{result.incident.error_message}\n"
                f"Root cause: "
                f"{result.incident.root_cause}\n"
                f"Applied fix: "
                f"{result.incident.fix_applied}\n"
                f"Confidence: "
                f"{result.incident.confidence}"
            )
        )

    return "\n\n".join(
        parts
    )


# ============================================================
# Rule-based fallback
# ============================================================

def _rule_based_diagnosis(
    context: dict,
    similar: list,
) -> Diagnosis:
    """
    Produce deterministic guidance when Ollama is unavailable.
    """

    error = str(
        context.get(
            "error_message",
            "",
        )
    )

    code = str(
        context.get(
            "code",
            "",
        )
    )

    error_lower = (
        error.lower()
    )

    for rule in (
        RULE_BASED_DIAGNOSES
    ):
        if any(
            pattern.lower()
            in error_lower
            for pattern
            in rule[
                "patterns"
            ]
        ):
            fixed_code = (
                code.rstrip()
                + "\n\n"
                + "# DataDoctor AI suggested remediation\n"
                + rule[
                    "fix_hint"
                ]
            )

            return Diagnosis(
                root_cause=(
                    rule[
                        "root_cause"
                    ]
                ),
                confidence=(
                    rule[
                        "confidence"
                    ]
                ),
                fixed_code=(
                    fixed_code
                ),
                source=(
                    "rule-based-local"
                ),
                similar_incidents=(
                    similar
                ),
                tools_used=[
                    "search_past_incidents"
                ],
                business_impact=(
                    "The failed step may delay downstream "
                    "datasets, reporting, analytics, and "
                    "pipeline SLA completion."
                ),
                validation=(
                    "Run the corrected step using controlled "
                    "test data, verify schema and row counts, "
                    "review logs, and confirm that rerunning "
                    "does not create duplicates."
                ),
            )

    if similar:
        best_match = (
            similar[0]
        )

        if (
            best_match.similarity_score
            >= 0.65
        ):
            return Diagnosis(
                root_cause=(
                    best_match
                    .incident
                    .root_cause
                ),
                confidence=(
                    best_match
                    .incident
                    .confidence
                    or "medium"
                ),
                fixed_code=(
                    best_match
                    .incident
                    .fix_applied
                    or code
                ),
                source=(
                    "rag-memory-fallback"
                ),
                similar_incidents=(
                    similar
                ),
                tools_used=[
                    "search_past_incidents"
                ],
                business_impact=(
                    "The failure may delay downstream data "
                    "availability and increase recovery effort."
                ),
                validation=(
                    "Validate the retrieved fix against the "
                    "current schema, code version, and target "
                    "environment before applying it."
                ),
            )

    return Diagnosis(
        root_cause=(
            "No sufficiently reliable known pattern matched "
            f"the current error: '{error}'. Review the full "
            "exception chain, schema, input data, recent "
            "changes, and Spark execution logs."
        ),
        confidence="low",
        fixed_code=code,
        source="rule-based-local",
        similar_incidents=similar,
        tools_used=[
            "search_past_incidents"
        ],
        business_impact=(
            "The unresolved failure may affect downstream "
            "data freshness and operational SLA completion."
        ),
        validation=(
            "Reproduce the failure with the same inputs, "
            "inspect the complete stack trace, validate the "
            "schema, and test any change before approval."
        ),
    )


# ============================================================
# Ollama prompt
# ============================================================

def _build_agent_question(
    context: dict,
    similar: list,
) -> str:
    """
    Build a complete evidence-grounded agent request.
    """

    recent_logs = (
        "\n".join(
            context.get(
                "recent_logs",
                [],
            )
        )
        or "No recent logs were recorded."
    )

    incident_memory = (
        _format_incident_memory(
            similar
        )
    )

    return (
        f"{AGENT_SYSTEM_PROMPT}\n\n"
        f"PIPELINE\n"
        f"Name: "
        f"{context.get('pipeline_name')}\n"
        f"Run status: "
        f"{context.get('run_status')}\n\n"
        f"FAILED STEP\n"
        f"ID: "
        f"{context.get('step_id')}\n"
        f"Name: "
        f"{context.get('step_name')}\n"
        f"Engine: "
        f"{context.get('engine')}\n"
        f"Retry count: "
        f"{context.get('retry_count')}\n\n"
        f"ERROR\n"
        f"{context.get('error_message')}\n\n"
        f"CURRENT CODE\n"
        f"{context.get('code')}\n\n"
        f"RECENT LOGS\n"
        f"{recent_logs}\n\n"
        f"RAG INCIDENT MEMORY\n"
        f"{incident_memory}\n\n"
        f"Perform an evidence-based diagnosis. Preserve the "
        f"original business logic unless a change is required "
        f"to resolve the demonstrated failure."
    )


# ============================================================
# LLM response parsing
# ============================================================

def _extract_section(
    text: str,
    section_name: str,
    next_sections: List[str],
) -> str:
    """
    Extract a named response section.
    """

    next_pattern = (
        "|".join(
            re.escape(
                section
            )
            for section
            in next_sections
        )
    )

    if next_pattern:
        pattern = (
            rf"{re.escape(section_name)}\s*:\s*"
            rf"(.*?)"
            rf"(?=\n(?:{next_pattern})\s*:|\Z)"
        )

    else:
        pattern = (
            rf"{re.escape(section_name)}\s*:\s*"
            rf"(.*)"
        )

    match = re.search(
        pattern,
        text,
        re.IGNORECASE
        | re.DOTALL,
    )

    return (
        match.group(1).strip()
        if match
        else ""
    )


def _clean_code(
    code: str,
) -> str:
    """
    Remove optional Markdown code fences.
    """

    cleaned = (
        code.strip()
    )

    cleaned = re.sub(
        r"^```[a-zA-Z0-9_+-]*\s*",
        "",
        cleaned,
    )

    cleaned = re.sub(
        r"\s*```$",
        "",
        cleaned,
    )

    return (
        cleaned.strip()
    )


def _parse_diagnosis(
    text: str,
    original_code: str,
    similar: list,
    tools_used: list,
    source: str,
) -> Diagnosis:
    """
    Parse the structured Ollama response safely.
    """

    root_cause = _extract_section(
        text,
        "ROOT_CAUSE",
        [
            "CONFIDENCE",
            "FIXED_CODE",
            "BUSINESS_IMPACT",
            "VALIDATION",
        ],
    )

    confidence = _extract_section(
        text,
        "CONFIDENCE",
        [
            "FIXED_CODE",
            "BUSINESS_IMPACT",
            "VALIDATION",
        ],
    )

    fixed_code = _extract_section(
        text,
        "FIXED_CODE",
        [
            "BUSINESS_IMPACT",
            "VALIDATION",
        ],
    )

    business_impact = (
        _extract_section(
            text,
            "BUSINESS_IMPACT",
            [
                "VALIDATION",
            ],
        )
    )

    validation = _extract_section(
        text,
        "VALIDATION",
        [],
    )

    normalized_confidence = (
        confidence
        .strip()
        .lower()
    )

    if normalized_confidence not in {
        "low",
        "medium",
        "high",
    }:
        normalized_confidence = (
            "low"
        )

    return Diagnosis(
        root_cause=(
            root_cause
            or "Review the complete model response."
        ),
        confidence=(
            normalized_confidence
        ),
        fixed_code=(
            _clean_code(
                fixed_code
            )
            or original_code
        ),
        source=source,
        similar_incidents=similar,
        tools_used=tools_used,
        business_impact=(
            business_impact
        ),
        validation=validation,
    )


# ============================================================
# Main diagnosis API
# ============================================================

def diagnose(
    run: dict,
    pipeline: dict,
    step_id: str,
) -> Diagnosis:
    """
    Run the complete agent workflow:

    1. Gather pipeline evidence.
    2. Retrieve similar resolved incidents.
    3. Ask free local Ollama for grounded reasoning.
    4. Fall back safely when Ollama is unavailable.
    """

    context = _gather_context(
        run,
        pipeline,
        step_id,
    )

    similar = (
        _retrieve_incident_memory(
            context,
            k=3,
        )
    )

    question = (
        _build_agent_question(
            context,
            similar,
        )
    )

    result = (
        ask_data_doctor(
            question=question,
            dataset_context={
                "pipeline_name": (
                    context[
                        "pipeline_name"
                    ]
                ),
                "step_name": (
                    context[
                        "step_name"
                    ]
                ),
                "engine": (
                    context[
                        "engine"
                    ]
                ),
                "retry_count": (
                    context[
                        "retry_count"
                    ]
                ),
                "run_status": (
                    context[
                        "run_status"
                    ]
                ),
            },
            rag_context=(
                _format_incident_memory(
                    similar
                )
            ),
            business_context={
                "required_outcome": (
                    "Restore pipeline execution while "
                    "preserving data correctness, "
                    "idempotency, and downstream SLA."
                )
            },
            analysis_context={
                "error_message": (
                    context[
                        "error_message"
                    ]
                ),
                "recent_logs": (
                    context[
                        "recent_logs"
                    ]
                ),
                "current_code": (
                    context[
                        "code"
                    ]
                ),
            },
            source_names=[
                "Pipeline definition",
                "Run history",
                "Execution logs",
                "Incident RAG memory",
            ],
            model=(
                DEFAULT_OLLAMA_MODEL
            ),
            base_url=(
                DEFAULT_OLLAMA_URL
            ),
            temperature=0.1,
        )
    )

    if not result.get(
        "success"
    ):
        return (
            _rule_based_diagnosis(
                context,
                similar,
            )
        )

    answer = str(
        result.get(
            "answer",
            "",
        )
    )

    if not answer.strip():
        return (
            _rule_based_diagnosis(
                context,
                similar,
            )
        )

    diagnosis = (
        _parse_diagnosis(
            text=answer,
            original_code=(
                context[
                    "code"
                ]
            ),
            similar=similar,
            tools_used=[
                "search_past_incidents",
                "ollama_reasoning",
            ],
            source=(
                "ollama-local-rag"
            ),
        )
    )

    if (
        not diagnosis.root_cause
        or diagnosis.root_cause
        == (
            "Review the complete model response."
        )
    ):
        return (
            _rule_based_diagnosis(
                context,
                similar,
            )
        )

    return diagnosis


# ============================================================
# Streaming-compatible diagnosis API
# ============================================================

def diagnose_stream(
    run: dict,
    pipeline: dict,
    step_id: str,
) -> Generator[Any, None, None]:
    """
    Compatibility generator for the existing Streamlit UI.

    The current Ollama assistant returns a completed grounded
    response. This function emits readable chunks and then a
    final diagnosis sentinel.

    Existing usage remains:

        for chunk in diagnose_stream(...):
            if isinstance(chunk, dict):
                diagnosis = chunk["diagnosis"]
            else:
                display_token(chunk)
    """

    diagnosis = diagnose(
        run,
        pipeline,
        step_id,
    )

    summary_parts = [
        (
            "ROOT_CAUSE: "
            f"{diagnosis.root_cause}"
        ),
        (
            "CONFIDENCE: "
            f"{diagnosis.confidence}"
        ),
    ]

    if diagnosis.business_impact:
        summary_parts.append(
            (
                "BUSINESS_IMPACT: "
                f"{diagnosis.business_impact}"
            )
        )

    if diagnosis.validation:
        summary_parts.append(
            (
                "VALIDATION: "
                f"{diagnosis.validation}"
            )
        )

    summary = (
        "\n\n".join(
            summary_parts
        )
    )

    chunk_size = 120

    for start in range(
        0,
        len(summary),
        chunk_size,
    ):
        yield summary[
            start:
            start + chunk_size
        ]

    yield {
        "diagnosis": diagnosis
    }


# ============================================================
# Apply approved fix
# ============================================================

def apply_fix(
    pipeline: dict,
    step_id: str,
    fixed_code: str,
) -> dict:
    """
    Patch the selected pipeline step and persist it.

    The fix is applied only when the UI explicitly calls this
    function after user approval.
    """

    if not isinstance(
        pipeline,
        dict,
    ):
        raise TypeError(
            "pipeline must be a dictionary."
        )

    steps = pipeline.get(
        "steps",
        [],
    )

    if not isinstance(
        steps,
        list,
    ):
        raise ValueError(
            "Pipeline steps must be a list."
        )

    updated = False

    for step in steps:
        if (
            isinstance(
                step,
                dict,
            )
            and step.get(
                "id"
            )
            == step_id
        ):
            step[
                "code"
            ] = fixed_code

            step[
                "updated_at"
            ] = (
                datetime.now(
                    timezone.utc
                )
                .isoformat()
            )

            updated = True

            break

    if not updated:
        raise ValueError(
            f"Pipeline step '{step_id}' was not found."
        )

    pipeline[
        "updated_at"
    ] = (
        datetime.now(
            timezone.utc
        )
        .isoformat()
    )

    store.save_pipeline(
        pipeline
    )

    try:
        store.append_audit_event(
            event_type=(
                "pipeline_fix"
            ),
            action=(
                "apply_agent_fix"
            ),
            status="success",
            details={
                "step_id": (
                    step_id
                ),
                "pipeline_name": (
                    pipeline.get(
                        "name",
                        "",
                    )
                ),
            },
            actor=(
                "DataDoctor AI"
            ),
            resource_id=(
                pipeline.get(
                    "id"
                )
            ),
        )

    except Exception:
        pass

    return pipeline


# ============================================================
# Store resolved incident in RAG memory
# ============================================================

def record_resolved_incident(
    run: dict,
    pipeline: dict,
    step_id: str,
    diagnosis: Diagnosis,
) -> Incident:
    """
    Store an approved resolved incident in RAG memory.

    Future diagnoses can retrieve this incident and reuse its
    root cause and remediation as grounded context.
    """

    pipeline_steps = (
        pipeline.get(
            "steps",
            [],
        )
        if isinstance(
            pipeline,
            dict,
        )
        else []
    )

    run_steps = (
        run.get(
            "step_runs",
            [],
        )
        if isinstance(
            run,
            dict,
        )
        else []
    )

    step = next(
        (
            candidate
            for candidate
            in pipeline_steps
            if isinstance(
                candidate,
                dict,
            )
            and candidate.get(
                "id"
            )
            == step_id
        ),
        {},
    )

    step_run = next(
        (
            candidate
            for candidate
            in run_steps
            if isinstance(
                candidate,
                dict,
            )
            and candidate.get(
                "step_id"
            )
            == step_id
        ),
        {},
    )

    now = (
        datetime.now(
            timezone.utc
        )
    )

    incident = Incident(
        id=(
            f"inc-{step_id}-"
            f"{now.strftime('%Y%m%d%H%M%S%f')}"
        ),
        error_message=str(
            step_run.get(
                "error_message"
            )
            or step_run.get(
                "error"
            )
            or ""
        ),
        step_code=str(
            step.get(
                "code",
                "",
            )
        )[:2000],
        root_cause=(
            diagnosis.root_cause
        ),
        fix_applied=(
            diagnosis.fixed_code[
                :4000
            ]
        ),
        pipeline_name=str(
            pipeline.get(
                "name",
                "",
            )
        ),
        resolved_at=(
            now.isoformat()
        ),
        confidence=(
            diagnosis.confidence
        ),
    )

    store_incident(
        incident
    )

    try:
        store.append_audit_event(
            event_type=(
                "rag_memory"
            ),
            action=(
                "store_resolved_incident"
            ),
            status="success",
            details={
                "incident_id": (
                    incident.id
                ),
                "step_id": (
                    step_id
                ),
                "confidence": (
                    diagnosis.confidence
                ),
                "diagnosis_source": (
                    diagnosis.source
                ),
            },
            actor=(
                "DataDoctor AI"
            ),
            resource_id=(
                pipeline.get(
                    "id"
                )
            ),
        )

    except Exception:
        pass

    return incident