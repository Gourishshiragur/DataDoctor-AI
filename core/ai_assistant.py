"""
DataDoctor AI
Enterprise AI Assistant

Supports:
- Free local Ollama models
- Grounded answers from uploaded data
- Enterprise RAG context
- Business-impact explanations
- PySpark and SQL code generation
- Zero-cost local code-template fallback
- Safe responses without hallucinated values
- Backward compatibility with existing pages
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


DEFAULT_OLLAMA_URL = os.getenv(
    "OLLAMA_BASE_URL",
    "http://localhost:11434",
).rstrip("/")

DEFAULT_OLLAMA_MODEL = os.getenv(
    "OLLAMA_MODEL",
    "llama3.2:3b",
)

DEFAULT_TIMEOUT = int(
    os.getenv(
        "LLM_TIMEOUT_SECONDS",
        "120",
    )
)


SYSTEM_PROMPT = """
You are DataDoctor AI, an enterprise data-engineering,
data-quality, analytics, and business decision-support
assistant.

Your responsibilities:

1. Use supplied dataset information, calculated metrics,
retrieved RAG context, schema information, quality findings,
and analysis results.

2. Never invent:

- Totals
- Averages
- Percentages
- Row counts
- Column names
- Business KPIs
- Anomalies
- Trends
- Source information

3. If the requested answer cannot be verified from supplied
context, clearly state that the available data is
insufficient.

4. Separate verified findings from assumptions.

5. Explain technical findings in business language.

6. When relevant, structure the response using:

Executive summary

Verified finding

Evidence

Business impact

Recommended action

Priority

7. Business outcomes may include:

- Revenue impact
- Cost impact
- Customer impact
- Operational impact
- Compliance risk
- SLA risk
- Reporting risk
- Decision-making risk

8. Do not claim exact financial impact unless supplied data
supports the calculation.

9. When RAG context is supplied, answer using that context
and mention source names when available.

10. Be concise while providing enough explanation for
business and technical users.

11. Use correct standard data-engineering terminology.
Always write "SCD Type 2", meaning "Slowly Changing
Dimension Type 2". Never rename it as "SDC Type 2".
Correct obvious spelling mistakes in the user's question
before answering.
""".strip()


@dataclass
class CodeSuggestion:
    """
    Structured code-generation response expected by the
    existing AI Code Assistant page.
    """

    code: str

    explanation: str

    source: str


def _safe_json(
    value: Any,
) -> str:
    """
    Convert Python values into readable JSON without failing
    on NumPy, pandas, timestamps, or custom objects.
    """

    try:

        return json.dumps(
            value,
            indent=2,
            ensure_ascii=False,
            default=str,
        )

    except Exception:

        return str(
            value
        )


def _clean_text(
    value: Any,
) -> str:

    if value is None:

        return ""

    return str(
        value
    ).strip()


def _normalise_messages(
    messages: Optional[
        List[
            Dict[
                str,
                Any,
            ]
        ]
    ],
) -> List[
    Dict[
        str,
        str,
    ]
]:
    """
    Keep valid chat roles and convert content to strings.
    """

    valid_roles = {
        "system",
        "user",
        "assistant",
    }

    cleaned: List[
        Dict[
            str,
            str,
        ]
    ] = []


    for message in (
        messages
        or []
    ):

        if not isinstance(
            message,
            dict,
        ):

            continue


        role = str(
            message.get(
                "role",
                "user",
            )
        ).lower()


        if role not in valid_roles:

            role = "user"


        content = _clean_text(
            message.get(
                "content"
            )
        )


        if content:

            cleaned.append(
                {
                    "role": role,
                    "content": content,
                }
            )


    return cleaned


def ollama_health(
    base_url: Optional[
        str
    ] = None,
    timeout: int = 5,
) -> Dict[
    str,
    Any,
]:
    """
    Check whether local Ollama is available.
    """

    url = (
        base_url
        or DEFAULT_OLLAMA_URL
    ).rstrip("/")


    try:

        response = requests.get(
            f"{url}/api/tags",
            timeout=timeout,
        )

        response.raise_for_status()

        payload = (
            response.json()
        )

        models = []


        for item in payload.get(
            "models",
            [],
        ):

            model_name = (
                item.get(
                    "name"
                )
                or item.get(
                    "model"
                )
            )


            if model_name:

                models.append(
                    model_name
                )


        return {
            "available": True,
            "provider": "Ollama",
            "base_url": url,
            "models": models,
            "message": (
                "Local Ollama service is available."
            ),
        }


    except Exception as exc:

        return {
            "available": False,
            "provider": "Ollama",
            "base_url": url,
            "models": [],
            "message": (
                "Ollama is not reachable. Start Ollama "
                "and make sure a model is installed."
            ),
            "error": str(
                exc
            ),
        }


def list_ollama_models(
    base_url: Optional[
        str
    ] = None,
) -> List[
    str
]:
    """
    Return installed local Ollama models.
    """

    result = ollama_health(
        base_url=base_url,
    )


    return result.get(
        "models",
        [],
    )


def build_grounded_prompt(
    question: str,
    dataset_context: Optional[
        Any
    ] = None,
    rag_context: Optional[
        Any
    ] = None,
    business_context: Optional[
        Any
    ] = None,
    analysis_context: Optional[
        Any
    ] = None,
    source_names: Optional[
        List[
            str
        ]
    ] = None,
) -> str:
    """
    Build a grounded prompt that separates verified
    application context from the user's question.
    """

    sections = [
        "USER QUESTION",
        _clean_text(
            question
        ),
    ]


    if dataset_context:

        sections.extend(
            [
                "",
                "VERIFIED DATASET CONTEXT",
                _safe_json(
                    dataset_context
                ),
            ]
        )


    if analysis_context:

        sections.extend(
            [
                "",
                "VERIFIED ANALYSIS RESULTS",
                _safe_json(
                    analysis_context
                ),
            ]
        )


    if rag_context:

        sections.extend(
            [
                "",
                "RETRIEVED RAG CONTEXT",
                _safe_json(
                    rag_context
                ),
            ]
        )


    if source_names:

        sections.extend(
            [
                "",
                "AVAILABLE SOURCE NAMES",
                _safe_json(
                    source_names
                ),
            ]
        )


    if business_context:

        sections.extend(
            [
                "",
                "BUSINESS CONTEXT",
                _safe_json(
                    business_context
                ),
            ]
        )


    sections.extend(
        [
            "",
            "ANSWERING RULES",
            (
                "Use only verified values from supplied "
                "context for data-specific claims."
            ),
            (
                "Do not create totals, averages, "
                "percentages, trends, anomalies, or "
                "financial impacts."
            ),
            (
                "If information is unavailable, state "
                "exactly what additional data is required."
            ),
            (
                "Explain findings, evidence, business "
                "impact, recommendations, and priority "
                "when relevant."
            ),
        ]
    )


    return "\n".join(
        sections
    )


def chat_with_ollama(
    prompt: str,
    model: Optional[
        str
    ] = None,
    base_url: Optional[
        str
    ] = None,
    system_prompt: Optional[
        str
    ] = None,
    messages: Optional[
        List[
            Dict[
                str,
                Any,
            ]
        ]
    ] = None,
    temperature: float = 0.1,
    timeout: Optional[
        int
    ] = None,
) -> Dict[
    str,
    Any,
]:
    """
    Send a chat request to a free local Ollama model.
    """

    selected_model = (
        model
        or DEFAULT_OLLAMA_MODEL
    )

    selected_url = (
        base_url
        or DEFAULT_OLLAMA_URL
    ).rstrip("/")


    chat_messages = [
        {
            "role": "system",
            "content": (
                system_prompt
                or SYSTEM_PROMPT
            ),
        }
    ]


    # Keep only recent conversation turns to reduce local
    # inference latency while preserving multi-turn context.
    recent_messages = (
        _normalise_messages(
            messages
        )[-6:]
    )

    chat_messages.extend(
        recent_messages
    )


    chat_messages.append(
        {
            "role": "user",
            "content": _clean_text(
                prompt
            ),
        }
    )


    payload = {
        "model": selected_model,
        "messages": chat_messages,
        "stream": False,
        "options": {
            "temperature": float(
                temperature
            ),
            # Limit prompt processing and response length for
            # faster local CPU inference.
            "num_ctx": 4096,
            "num_predict": 600,
        },
        # Keep the model loaded between requests so later
        # questions do not repeatedly pay model-load cost.
        "keep_alive": "30m",
    }


    try:

        response = requests.post(
            (
                f"{selected_url}"
                "/api/chat"
            ),
            json=payload,
            timeout=(
                timeout
                or DEFAULT_TIMEOUT
            ),
        )


        response.raise_for_status()


        data = (
            response.json()
        )


        answer = (
            data
            .get(
                "message",
                {},
            )
            .get(
                "content",
                "",
            )
            .strip()
        )


        if not answer:

            answer = (
                "The local model returned an empty "
                "response."
            )


        return {
            "success": True,
            "answer": answer,
            "provider": "Ollama",
            "model": selected_model,
            "grounded": True,
            "error": None,
        }


    except requests.exceptions.ConnectionError:

        return {
            "success": False,
            "answer": (
                "The local Ollama service is not running. "
                "Start Ollama, install a model, and try "
                "again."
            ),
            "provider": "Ollama",
            "model": selected_model,
            "grounded": False,
            "error": (
                "OLLAMA_CONNECTION_ERROR"
            ),
        }


    except requests.exceptions.Timeout:

        return {
            "success": False,
            "answer": (
                "The local model request timed out. "
                "Try a smaller model or increase "
                "LLM_TIMEOUT_SECONDS."
            ),
            "provider": "Ollama",
            "model": selected_model,
            "grounded": False,
            "error": (
                "OLLAMA_TIMEOUT"
            ),
        }


    except requests.exceptions.HTTPError as exc:

        error_text = ""


        try:

            error_text = (
                response.text
            )


        except Exception:

            error_text = str(
                exc
            )


        return {
            "success": False,
            "answer": (
                "Ollama returned an error. Confirm that "
                f"model '{selected_model}' is installed."
            ),
            "provider": "Ollama",
            "model": selected_model,
            "grounded": False,
            "error": error_text,
        }


    except Exception as exc:

        return {
            "success": False,
            "answer": (
                "The AI request could not be completed. "
                "No unverified data answer was generated."
            ),
            "provider": "Ollama",
            "model": selected_model,
            "grounded": False,
            "error": str(
                exc
            ),
        }


def ask_data_doctor(
    question: str,
    dataset_context: Optional[
        Any
    ] = None,
    rag_context: Optional[
        Any
    ] = None,
    business_context: Optional[
        Any
    ] = None,
    analysis_context: Optional[
        Any
    ] = None,
    source_names: Optional[
        List[
            str
        ]
    ] = None,
    model: Optional[
        str
    ] = None,
    base_url: Optional[
        str
    ] = None,
    chat_history: Optional[
        List[
            Dict[
                str,
                Any,
            ]
        ]
    ] = None,
    temperature: float = 0.1,
) -> Dict[
    str,
    Any,
]:
    """
    Main enterprise assistant function.
    """

    cleaned_question = (
        _clean_text(
            question
        )
    )


    if not cleaned_question:

        return {
            "success": False,
            "answer": (
                "Enter a question before sending."
            ),
            "provider": (
                "DataDoctor AI"
            ),
            "model": model,
            "grounded": False,
            "error": (
                "EMPTY_QUESTION"
            ),
        }


    grounded_prompt = (
        build_grounded_prompt(
            question=(
                cleaned_question
            ),
            dataset_context=(
                dataset_context
            ),
            rag_context=(
                rag_context
            ),
            business_context=(
                business_context
            ),
            analysis_context=(
                analysis_context
            ),
            source_names=(
                source_names
            ),
        )
    )


    result = chat_with_ollama(
        prompt=(
            grounded_prompt
        ),
        model=model,
        base_url=base_url,
        messages=(
            chat_history
        ),
        temperature=(
            temperature
        ),
    )


    result[
        "rag_used"
    ] = bool(
        rag_context
    )


    result[
        "dataset_context_used"
    ] = bool(
        dataset_context
        or analysis_context
    )


    result[
        "sources"
    ] = (
        source_names
        or []
    )


    return result


def generate_business_explanation(
    findings: Any,
    model: Optional[
        str
    ] = None,
    base_url: Optional[
        str
    ] = None,
) -> Dict[
    str,
    Any,
]:
    """
    Convert verified technical findings into an executive
    and business-oriented explanation.
    """

    prompt = f"""
Create an enterprise business explanation for the verified
technical findings below.

VERIFIED FINDINGS

{_safe_json(findings)}

Include:

1. Executive summary
2. Key verified findings
3. Why the findings matter
4. Possible business impact
5. Recommended actions
6. Priority

Do not invent financial values or unsupported causes.
Clearly label anything requiring further investigation.
""".strip()


    return chat_with_ollama(
        prompt=prompt,
        model=model,
        base_url=base_url,
        temperature=0.1,
    )


def generate_data_quality_explanation(
    quality_report: Any,
    model: Optional[
        str
    ] = None,
    base_url: Optional[
        str
    ] = None,
) -> Dict[
    str,
    Any,
]:
    """
    Explain an already calculated data-quality report.
    """

    prompt = f"""
Explain the following calculated data-quality report.

CALCULATED QUALITY REPORT

{_safe_json(quality_report)}

Explain:

- Overall health
- Critical issues
- Affected business decisions
- Operational or reporting risks
- Recommended remediation
- Suggested validation checks

Use only supplied values.
Do not calculate or invent missing metrics.
""".strip()


    return chat_with_ollama(
        prompt=prompt,
        model=model,
        base_url=base_url,
        temperature=0.1,
    )


def generate_root_cause_explanation(
    issue: Any,
    evidence: Any,
    model: Optional[
        str
    ] = None,
    base_url: Optional[
        str
    ] = None,
) -> Dict[
    str,
    Any,
]:
    """
    Generate evidence-aware root-cause guidance.
    """

    prompt = f"""
Analyse the following enterprise data issue.

ISSUE

{_safe_json(issue)}

AVAILABLE EVIDENCE

{_safe_json(evidence)}

Return:

1. Observed issue
2. Evidence
3. Likely causes
4. Confidence level
5. Business impact
6. Recommended diagnostic steps
7. Recommended remediation

Do not present a likely cause as confirmed unless evidence
proves it.
""".strip()


    return chat_with_ollama(
        prompt=prompt,
        model=model,
        base_url=base_url,
        temperature=0.1,
    )


def _clean_generated_code(
    code: str,
) -> str:
    """
    Remove optional Markdown fences returned by an LLM.
    """

    cleaned = str(
        code
        or ""
    ).strip()


    cleaned = re.sub(
        (
            r"^```"
            r"[a-zA-Z0-9_+-]*"
            r"\s*"
        ),
        "",
        cleaned,
    )


    cleaned = re.sub(
        r"\s*```$",
        "",
        cleaned,
    )


    return cleaned.strip()


def _extract_generated_sections(
    answer: str,
) -> tuple[
    str,
    str,
]:
    """
    Extract EXPLANATION and CODE from the Ollama response.
    """

    response_text = str(
        answer
        or ""
    ).strip()


    explanation_match = re.search(
        (
            r"EXPLANATION\s*:\s*"
            r"(.*?)"
            r"(?=\nCODE\s*:|\Z)"
        ),
        response_text,
        flags=(
            re.IGNORECASE
            | re.DOTALL
        ),
    )


    code_match = re.search(
        (
            r"CODE\s*:\s*"
            r"(.*)"
        ),
        response_text,
        flags=(
            re.IGNORECASE
            | re.DOTALL
        ),
    )


    explanation = (
        explanation_match
        .group(
            1
        )
        .strip()
        if explanation_match
        else (
            "Generated using the configured free local "
            "Ollama model."
        )
    )


    code = (
        code_match
        .group(
            1
        )
        .strip()
        if code_match
        else response_text
    )


    return (
        explanation,
        _clean_generated_code(
            code
        ),
    )


def _local_code_template(
    prompt: str,
    engine: str,
) -> CodeSuggestion:
    """
    Zero-cost deterministic code fallback.

    Produces common enterprise PySpark and SQL patterns when
    Ollama is unavailable, missing, or times out.
    """

    request = str(
        prompt
        or ""
    ).strip()


    request_lower = (
        request.lower()
    )


    normalized_engine = (
        str(
            engine
            or "pyspark"
        )
        .strip()
        .lower()
    )


    if normalized_engine not in {
        "pyspark",
        "sql",
    }:

        normalized_engine = (
            "pyspark"
        )


    if (
        "scd2"
        in request_lower
        or "scd type 2"
        in request_lower
        or "slowly changing dimension"
        in request_lower
    ):

        if (
            normalized_engine
            == "sql"
        ):

            return CodeSuggestion(
                code=(
                    "MERGE INTO target_dimension AS target\n"
                    "USING staged_changes AS source\n"
                    "ON target.business_key = "
                    "source.business_key\n"
                    "AND target.is_current = TRUE\n\n"
                    "WHEN MATCHED\n"
                    "AND target.attribute_hash <> "
                    "source.attribute_hash\n"
                    "THEN UPDATE SET\n"
                    "    target.is_current = FALSE,\n"
                    "    target.effective_to = "
                    "source.effective_from\n\n"
                    "WHEN NOT MATCHED\n"
                    "THEN INSERT (\n"
                    "    business_key,\n"
                    "    attribute_hash,\n"
                    "    effective_from,\n"
                    "    effective_to,\n"
                    "    is_current\n"
                    ")\n"
                    "VALUES (\n"
                    "    source.business_key,\n"
                    "    source.attribute_hash,\n"
                    "    source.effective_from,\n"
                    "    TIMESTAMP "
                    "'9999-12-31 23:59:59',\n"
                    "    TRUE\n"
                    ");"
                ),
                explanation=(
                    "Provides an SCD Type 2 starting pattern "
                    "that closes changed current records. "
                    "Production implementation must also "
                    "insert changed records as new current "
                    "versions."
                ),
                source=(
                    "local-template"
                ),
            )


        return CodeSuggestion(
            code=(
                "from delta.tables import DeltaTable\n"
                "from pyspark.sql.functions import (\n"
                "    col,\n"
                "    current_timestamp,\n"
                "    lit,\n"
                ")\n\n"
                "target = DeltaTable.forPath(\n"
                "    spark,\n"
                "    target_path,\n"
                ")\n\n"
                "current_target_df = (\n"
                "    target\n"
                "    .toDF()\n"
                "    .filter(\n"
                "        col('is_current') == lit(True)\n"
                "    )\n"
                ")\n\n"
                "changed_records = (\n"
                "    source_df.alias('source')\n"
                "    .join(\n"
                "        current_target_df.alias('target'),\n"
                "        col('source.business_key')\n"
                "        == col('target.business_key'),\n"
                "        'left',\n"
                "    )\n"
                "    .filter(\n"
                "        col(\n"
                "            'target.business_key'\n"
                "        ).isNull()\n"
                "        | (\n"
                "            col(\n"
                "                'source.attribute_hash'\n"
                "            )\n"
                "            != col(\n"
                "                'target.attribute_hash'\n"
                "            )\n"
                "        )\n"
                "    )\n"
                ")\n\n"
                "(\n"
                "    target.alias('target')\n"
                "    .merge(\n"
                "        changed_records.alias('source'),\n"
                "        'target.business_key = '\n"
                "        'source.business_key AND '\n"
                "        'target.is_current = true',\n"
                "    )\n"
                "    .whenMatchedUpdate(\n"
                "        condition=(\n"
                "            'target.attribute_hash <> '\n"
                "            'source.attribute_hash'\n"
                "        ),\n"
                "        set={\n"
                "            'is_current': 'false',\n"
                "            'effective_to': (\n"
                "                'current_timestamp()'\n"
                "            ),\n"
                "        },\n"
                "    )\n"
                "    .execute()\n"
                ")\n\n"
                "new_versions = (\n"
                "    changed_records\n"
                "    .select('source.*')\n"
                "    .withColumn(\n"
                "        'effective_from',\n"
                "        current_timestamp(),\n"
                "    )\n"
                "    .withColumn(\n"
                "        'effective_to',\n"
                "        lit(None).cast('timestamp'),\n"
                "    )\n"
                "    .withColumn(\n"
                "        'is_current',\n"
                "        lit(True),\n"
                "    )\n"
                ")\n\n"
                "(\n"
                "    new_versions\n"
                "    .write\n"
                "    .format('delta')\n"
                "    .mode('append')\n"
                "    .save(target_path)\n"
                ")"
            ),
            explanation=(
                "Closes changed current records and appends "
                "new current versions while preserving "
                "historical dimension records."
            ),
            source=(
                "local-template"
            ),
        )


    if (
        "deduplicate"
        in request_lower
        or "deduplication"
        in request_lower
        or "latest record"
        in request_lower
        or "retain the latest"
        in request_lower
    ):

        if (
            normalized_engine
            == "sql"
        ):

            return CodeSuggestion(
                code=(
                    "WITH ranked_records AS (\n"
                    "    SELECT\n"
                    "        source.*,\n"
                    "        ROW_NUMBER() OVER (\n"
                    "            PARTITION BY natural_key\n"
                    "            ORDER BY\n"
                    "                event_time DESC,\n"
                    "                "
                    "ingestion_timestamp DESC\n"
                    "        ) AS row_number\n"
                    "    FROM source_table AS source\n"
                    ")\n\n"
                    "SELECT\n"
                    "    * EXCEPT (row_number)\n"
                    "FROM ranked_records\n"
                    "WHERE row_number = 1;"
                ),
                explanation=(
                    "Uses ROW_NUMBER to retain the newest "
                    "record for each natural key. The "
                    "ingestion timestamp provides a "
                    "deterministic tie-breaker."
                ),
                source=(
                    "local-template"
                ),
            )


        return CodeSuggestion(
            code=(
                "from pyspark.sql import Window\n"
                "from pyspark.sql.functions import (\n"
                "    col,\n"
                "    row_number,\n"
                ")\n\n"
                "latest_record_window = (\n"
                "    Window\n"
                "    .partitionBy(\n"
                "        'natural_key'\n"
                "    )\n"
                "    .orderBy(\n"
                "        col(\n"
                "            'event_time'\n"
                "        ).desc(),\n"
                "        col(\n"
                "            'ingestion_timestamp'\n"
                "        ).desc(),\n"
                "    )\n"
                ")\n\n"
                "deduplicated_df = (\n"
                "    source_df\n"
                "    .withColumn(\n"
                "        '_row_number',\n"
                "        row_number().over(\n"
                "            latest_record_window\n"
                "        ),\n"
                "    )\n"
                "    .filter(\n"
                "        col('_row_number') == 1\n"
                "    )\n"
                "    .drop(\n"
                "        '_row_number'\n"
                "    )\n"
                ")"
            ),
            explanation=(
                "Uses a PySpark window function to retain "
                "the latest record for each natural key with "
                "a deterministic ingestion-time tie-breaker."
            ),
            source=(
                "local-template"
            ),
        )


    if (
        "watermark"
        in request_lower
        or "incremental load"
        in request_lower
        or "incremental"
        in request_lower
    ):

        if (
            normalized_engine
            == "sql"
        ):

            return CodeSuggestion(
                code=(
                    "WITH current_watermark AS (\n"
                    "    SELECT\n"
                    "        COALESCE(\n"
                    "            MAX(\n"
                    "                last_processed_value\n"
                    "            ),\n"
                    "            TIMESTAMP "
                    "'1900-01-01 00:00:00'\n"
                    "        ) AS watermark_value\n"
                    "    FROM pipeline_control\n"
                    "    WHERE pipeline_name = "
                    "'target_pipeline'\n"
                    ")\n\n"
                    "SELECT\n"
                    "    source.*\n"
                    "FROM source_table AS source\n"
                    "CROSS JOIN current_watermark\n"
                    "WHERE source.updated_at\n"
                    "    > current_watermark."
                    "watermark_value;"
                ),
                explanation=(
                    "Reads only records newer than the "
                    "stored watermark. Update the control "
                    "table only after the target write and "
                    "validation complete successfully."
                ),
                source=(
                    "local-template"
                ),
            )


        return CodeSuggestion(
            code=(
                "from pyspark.sql.functions import (\n"
                "    col,\n"
                "    lit,\n"
                ")\n\n"
                "watermark_row = (\n"
                "    control_df\n"
                "    .filter(\n"
                "        col('pipeline_name')\n"
                "        == lit('target_pipeline')\n"
                "    )\n"
                "    .select(\n"
                "        'last_processed_value'\n"
                "    )\n"
                "    .first()\n"
                ")\n\n"
                "last_processed_value = (\n"
                "    watermark_row[\n"
                "        'last_processed_value'\n"
                "    ]\n"
                "    if watermark_row\n"
                "    else '1900-01-01 00:00:00'\n"
                ")\n\n"
                "incremental_df = (\n"
                "    source_df\n"
                "    .filter(\n"
                "        col('updated_at')\n"
                "        > lit(\n"
                "            last_processed_value\n"
                "        )\n"
                "    )\n"
                ")"
            ),
            explanation=(
                "Filters source records using the last "
                "successful watermark. Persist the new "
                "watermark only after target validation and "
                "successful pipeline completion."
            ),
            source=(
                "local-template"
            ),
        )


    if (
        "merge"
        in request_lower
        or "upsert"
        in request_lower
    ):

        if (
            normalized_engine
            == "sql"
        ):

            return CodeSuggestion(
                code=(
                    "MERGE INTO target_table AS target\n"
                    "USING source_view AS source\n"
                    "ON target.device_id = "
                    "source.device_id\n"
                    "AND target.event_date = "
                    "source.event_date\n\n"
                    "WHEN MATCHED THEN\n"
                    "UPDATE SET *\n\n"
                    "WHEN NOT MATCHED THEN\n"
                    "INSERT *;"
                ),
                explanation=(
                    "Performs a Delta MERGE using device_id "
                    "and event_date. Validate and deduplicate "
                    "the source before the MERGE."
                ),
                source=(
                    "local-template"
                ),
            )


        return CodeSuggestion(
            code=(
                "from delta.tables import DeltaTable\n\n"
                "target = DeltaTable.forPath(\n"
                "    spark,\n"
                "    target_path,\n"
                ")\n\n"
                "(\n"
                "    target.alias('target')\n"
                "    .merge(\n"
                "        source_df.alias('source'),\n"
                "        'target.device_id = '\n"
                "        'source.device_id AND '\n"
                "        'target.event_date = '\n"
                "        'source.event_date',\n"
                "    )\n"
                "    .whenMatchedUpdateAll()\n"
                "    .whenNotMatchedInsertAll()\n"
                "    .execute()\n"
                ")"
            ),
            explanation=(
                "Performs a Delta Lake upsert using "
                "device_id and event_date. Validate and "
                "deduplicate source records before MERGE."
            ),
            source=(
                "local-template"
            ),
        )


    if (
        normalized_engine
        == "sql"
    ):

        return CodeSuggestion(
            code=(
                "SELECT\n"
                "    *\n"
                "FROM source_table\n"
                "WHERE required_column IS NOT NULL;"
            ),
            explanation=(
                "Generated a safe SQL starting pattern. "
                "Replace placeholder tables and columns only "
                "with fields verified in the source schema."
            ),
            source=(
                "local-template"
            ),
        )


    return CodeSuggestion(
        code=(
            "from pyspark.sql.functions import col\n\n"
            "validated_df = (\n"
            "    source_df\n"
            "    .filter(\n"
            "        col(\n"
            "            'required_column'\n"
            "        ).isNotNull()\n"
            "    )\n"
            ")"
        ),
        explanation=(
            "Generated a safe PySpark starting pattern. "
            "Replace placeholder columns only with verified "
            "fields from the source schema."
        ),
        source=(
            "local-template"
        ),
    )


def get_code_suggestion(
    prompt: str,
    engine: str = "pyspark",
    model: Optional[
        str
    ] = None,
    **kwargs: Any,
) -> CodeSuggestion:
    """
    Generate a structured PySpark or SQL suggestion.

    Compatible with the existing page:

    get_code_suggestion(
        prompt="...",
        engine="pyspark",
    )

    Returns:

    suggestion.code

    suggestion.explanation

    suggestion.source
    """

    cleaned_prompt = str(
        prompt
        or ""
    ).strip()


    normalized_engine = (
        str(
            engine
            or "pyspark"
        )
        .strip()
        .lower()
    )


    if normalized_engine not in {
        "pyspark",
        "sql",
    }:

        normalized_engine = (
            "pyspark"
        )


    if not cleaned_prompt:

        return CodeSuggestion(
            code="",
            explanation=(
                "Describe the required transformation or "
                "data-engineering pattern first."
            ),
            source=(
                "local-template"
            ),
        )


    generation_prompt = f"""
You are DataDoctor AI, an enterprise data-engineering code
assistant.

Generate a production-oriented {normalized_engine.upper()}
solution for this requirement:

{cleaned_prompt}

Return exactly:

EXPLANATION:

A concise explanation of the implementation.

CODE:

Complete executable {normalized_engine.upper()} code.

Requirements:

- Preserve requested business logic.
- Use only necessary imports.
- Include relevant schema, null, duplicate, and idempotency
  safeguards when appropriate.
- Consider maintainability, performance, auditability,
  scalability, and pipeline reliability.
- Do not invent execution results, business metrics, or
  unsupported source information.
- Do not include Markdown code fences.
""".strip()


    try:

        result = chat_with_ollama(
            prompt=(
                generation_prompt
            ),
            model=model,
            base_url=kwargs.get(
                "base_url"
            ),
            temperature=kwargs.get(
                "temperature",
                0.1,
            ),
            timeout=kwargs.get(
                "timeout",
                20,
            ),
        )


        if result.get(
            "success"
        ):

            explanation, code = (
                _extract_generated_sections(
                    result.get(
                        "answer",
                        "",
                    )
                )
            )


            if code:

                return CodeSuggestion(
                    code=code,
                    explanation=(
                        explanation
                    ),
                    source=(
                        "ollama-local"
                    ),
                )


    except Exception:

        pass


    fallback = (
        _local_code_template(
            prompt=(
                cleaned_prompt
            ),
            engine=(
                normalized_engine
            ),
        )
    )


    return CodeSuggestion(
        code=(
            fallback.code
        ),
        explanation=(
            fallback.explanation
        ),
        source=(
            "fallback-after-api"
        ),
    )


def ask_ai(
    prompt: str,
    context: Optional[
        Any
    ] = None,
    model: Optional[
        str
    ] = None,
    **kwargs: Any,
) -> str:
    """
    Backward-compatible string-returning helper.
    """

    result = ask_data_doctor(
        question=prompt,
        dataset_context=(
            context
        ),
        model=model,
        base_url=kwargs.get(
            "base_url"
        ),
        chat_history=kwargs.get(
            "messages"
        ),
        temperature=kwargs.get(
            "temperature",
            0.1,
        ),
    )


    return result.get(
        "answer",
        (
            "No response was generated."
        ),
    )


def chat(
    prompt: str,
    context: Optional[
        Any
    ] = None,
    model: Optional[
        str
    ] = None,
    **kwargs: Any,
) -> str:
    """
    Compatibility alias for older application code.
    """

    return ask_ai(
        prompt=prompt,
        context=context,
        model=model,
        **kwargs,
    )


def generate_response(
    prompt: str,
    context: Optional[
        Any
    ] = None,
    model: Optional[
        str
    ] = None,
    **kwargs: Any,
) -> str:
    """
    Compatibility alias for older application code.
    """

    return ask_ai(
        prompt=prompt,
        context=context,
        model=model,
        **kwargs,
    )