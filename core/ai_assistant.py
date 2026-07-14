"""
AI-powered PySpark and SQL code suggestions.

Uses the Anthropic API when a valid API key with available credits is
configured. If the API is unavailable, has insufficient credits, or no
key is configured, DataDoctor AI automatically uses its zero-cost local
template engine.

The local fallback always respects the selected engine:
  - PySpark returns PySpark code
  - SQL returns SQL code
"""

import os
from dataclasses import dataclass

import requests


ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"

MODEL = "claude-sonnet-4-6"


SYSTEM_PROMPT = (
    "You are a senior data engineer embedded in DataDoctor AI. "
    "Generate code only for the engine explicitly selected by the user. "
    "If Engine is PySpark, return valid PySpark/Python code and never SQL-only code. "
    "If Engine is SQL, return valid SQL code and never PySpark or Python code. "
    "Return one practical code snippet of approximately 30 lines or fewer, "
    "followed by no unnecessary discussion. "
    "Prefer production-oriented Delta Lake patterns where relevant. "
    "Output exactly in this format:\n\n"
    "EXPLANATION: <one concise sentence>\n"
    "```<selected language>\n"
    "<code>\n"
    "```"
)


# =========================================================
# PYSPARK LOCAL TEMPLATES
# =========================================================

PYSPARK_TEMPLATES = {

    "incremental": (
        "Comparison-based incremental ingestion using a watermark "
        "to select only new or changed records.",
        """```python
from pyspark.sql import functions as F

latest_watermark = (
    spark.read.table("pipeline_control")
    .filter(F.col("pipeline_name") == "telemetry_pipeline")
    .select(F.max("watermark_value"))
    .first()[0]
)

incremental_df = (
    spark.read.table("source_events")
    .filter(F.col("updated_at") > F.lit(latest_watermark))
)

incremental_df.write.format("delta").mode("append").saveAsTable(
    "bronze.incremental_events"
)
```""",
    ),

    "scd2": (
        "Delta Lake SCD Type 2 pattern that closes changed current "
        "records before inserting new versions.",
        """```python
from delta.tables import DeltaTable

target = DeltaTable.forName(
    spark,
    "silver.dim_device"
)

(
    target.alias("t")
    .merge(
        source_df.alias("s"),
        "t.device_id = s.device_id AND t.is_current = true"
    )
    .whenMatchedUpdate(
        condition="t.status <> s.status",
        set={
            "is_current": "false",
            "effective_to": "s.event_time"
        }
    )
    .execute()
)
```""",
    ),

    "dedup": (
        "Window-based deduplication that retains the newest event "
        "for every device.",
        """```python
from pyspark.sql import Window
from pyspark.sql import functions as F

latest_event_window = (
    Window
    .partitionBy("device_id")
    .orderBy(F.col("event_time").desc())
)

deduplicated_df = (
    source_df
    .withColumn(
        "row_number",
        F.row_number().over(latest_event_window)
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
```""",
    ),

    "merge": (
        "Idempotent Delta Lake MERGE that updates existing records "
        "and inserts new records.",
        """```python
from delta.tables import DeltaTable

target = DeltaTable.forName(
    spark,
    "silver.device_events"
)

(
    target.alias("t")
    .merge(
        source_df.alias("s"),
        "t.device_id = s.device_id AND t.event_date = s.event_date"
    )
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```""",
    ),

    "aggregation": (
        "PySpark aggregation that calculates daily event metrics "
        "for every device.",
        """```python
from pyspark.sql import functions as F

daily_metrics_df = (
    source_df
    .withColumn(
        "event_day",
        F.to_date("event_time")
    )
    .groupBy(
        "device_id",
        "event_day"
    )
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_time").alias("latest_event_time")
    )
    .orderBy(
        F.col("event_day").desc()
    )
)
```""",
    ),

    "default": (
        "Production-style PySpark transformation with validation, "
        "deduplication, and standardized output columns.",
        """```python
from pyspark.sql import Window
from pyspark.sql import functions as F

window_spec = (
    Window
    .partitionBy("device_id")
    .orderBy(F.col("event_time").desc())
)

result_df = (
    source_df
    .filter(F.col("device_id").isNotNull())
    .withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
    .filter(F.col("row_number") == 1)
    .drop("row_number")
    .withColumn(
        "processed_at",
        F.current_timestamp()
    )
)
```""",
    ),
}


# =========================================================
# SQL LOCAL TEMPLATES
# =========================================================

SQL_TEMPLATES = {

    "incremental": (
        "SQL incremental-load pattern that reads records newer "
        "than the stored pipeline watermark.",
        """```sql
WITH current_watermark AS (
    SELECT
        MAX(watermark_value) AS watermark_value
    FROM pipeline_control
    WHERE pipeline_name = 'telemetry_pipeline'
)

SELECT
    source.*
FROM source_events AS source
CROSS JOIN current_watermark AS control
WHERE source.updated_at > control.watermark_value;
```""",
    ),

    "scd2": (
        "Delta SQL SCD Type 2 pattern that closes changed current "
        "dimension records.",
        """```sql
MERGE INTO silver.dim_device AS target
USING staging.device_updates AS source
ON target.device_id = source.device_id
AND target.is_current = TRUE

WHEN MATCHED
AND target.status <> source.status
THEN UPDATE SET
    target.is_current = FALSE,
    target.effective_to = source.event_time;
```""",
    ),

    "dedup": (
        "SQL window-function pattern that retains the newest event "
        "for every device.",
        """```sql
WITH ranked_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY device_id
            ORDER BY event_time DESC
        ) AS row_number
    FROM source_events
)

SELECT
    *
FROM ranked_events
WHERE row_number = 1;
```""",
    ),

    "merge": (
        "Delta SQL MERGE that updates matching records and inserts "
        "new records.",
        """```sql
MERGE INTO silver.device_events AS target
USING staging.device_events AS source
ON target.device_id = source.device_id
AND target.event_date = source.event_date

WHEN MATCHED THEN
    UPDATE SET *

WHEN NOT MATCHED THEN
    INSERT *;
```""",
    ),

    "aggregation": (
        "SQL aggregation that calculates daily event metrics for "
        "every device.",
        """```sql
SELECT
    device_id,
    DATE_TRUNC(
        'DAY',
        event_time
    ) AS event_day,
    COUNT(*) AS event_count,
    MAX(event_time) AS latest_event_time
FROM source_events
GROUP BY
    device_id,
    DATE_TRUNC(
        'DAY',
        event_time
    )
ORDER BY
    event_day DESC;
```""",
    ),

    "default": (
        "Production-style SQL transformation that validates and "
        "deduplicates source events.",
        """```sql
WITH valid_events AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY device_id
            ORDER BY event_time DESC
        ) AS row_number
    FROM source_events
    WHERE device_id IS NOT NULL
)

SELECT
    *
FROM valid_events
WHERE row_number = 1;
```""",
    ),
}


# =========================================================
# RESPONSE MODEL
# =========================================================

@dataclass
class Suggestion:

    explanation: str

    code: str

    source: str


# =========================================================
# API KEY
# =========================================================

def _get_api_key() -> str | None:

    try:

        import streamlit as st

        if "ANTHROPIC_API_KEY" in st.secrets:

            return st.secrets[
                "ANTHROPIC_API_KEY"
            ]

    except Exception:

        pass


    return os.environ.get(
        "ANTHROPIC_API_KEY"
    )


# =========================================================
# NORMALIZE ENGINE
# =========================================================

def _normalize_engine(
    engine: str,
) -> str:

    normalized = (

        str(
            engine
            or "pyspark"
        )
        .strip()
        .lower()

    )


    if normalized in (

        "sql",
        "spark sql",
        "delta sql",

    ):

        return "sql"


    return "pyspark"


# =========================================================
# DETECT REQUEST PATTERN
# =========================================================

def _detect_pattern(
    prompt: str,
) -> str:

    prompt_lower = (

        prompt.lower()

    )


    if any(

        keyword in prompt_lower

        for keyword in (

            "scd2",
            "scd 2",
            "scd type 2",
            "slowly changing dimension",

        )

    ):

        return "scd2"


    if any(

        keyword in prompt_lower

        for keyword in (

            "dedup",
            "duplicate",
            "latest record",
            "latest event",

        )

    ):

        return "dedup"


    if any(

        keyword in prompt_lower

        for keyword in (

            "merge",
            "upsert",

        )

    ):

        return "merge"


    if any(

        keyword in prompt_lower

        for keyword in (

            "incremental",
            "watermark",
            "new records",
            "changed records",

        )

    ):

        return "incremental"


    if any(

        keyword in prompt_lower

        for keyword in (

            "aggregate",
            "aggregation",
            "group by",
            "count",
            "daily metric",

        )

    ):

        return "aggregation"


    return "default"


# =========================================================
# LOCAL ZERO-COST FALLBACK
# =========================================================

def _fallback_suggestion(
    prompt: str,
    engine: str = "pyspark",
) -> Suggestion:

    selected_engine = (

        _normalize_engine(
            engine
        )

    )


    pattern = (

        _detect_pattern(
            prompt
        )

    )


    if selected_engine == "sql":

        templates = SQL_TEMPLATES

    else:

        templates = PYSPARK_TEMPLATES


    explanation, code = (

        templates.get(

            pattern,

            templates["default"],

        )

    )


    return Suggestion(

        explanation=explanation,

        code=code,

        source=(
            f"local-{selected_engine}-template"
        ),

    )


# =========================================================
# PARSE ANTHROPIC RESPONSE
# =========================================================

def _parse_response_text(
    text: str,
) -> Suggestion:

    explanation = ""

    code = text


    if "EXPLANATION:" in text:


        after_explanation = (

            text.split(

                "EXPLANATION:",

                1,

            )[1]

        )


        if "```" in after_explanation:


            explanation, code_body = (

                after_explanation.split(

                    "```",

                    1,

                )

            )


            code = (

                "```"

                + code_body

            )


        else:


            explanation = (

                after_explanation

            )


    return Suggestion(

        explanation=(

            explanation.strip()

        ),

        code=(

            code.strip()

        ),

        source="claude-api",

    )


# =========================================================
# PUBLIC CODE-SUGGESTION FUNCTION
# =========================================================

def get_code_suggestion(
    prompt: str,
    engine: str = "pyspark",
) -> Suggestion:

    selected_engine = (

        _normalize_engine(
            engine
        )

    )


    api_key = (

        _get_api_key()

    )


    # No API key:
    # use the selected SQL or PySpark local engine.

    if not api_key:


        return _fallback_suggestion(

            prompt,

            selected_engine,

        )


    selected_language = (

        "SQL"

        if selected_engine == "sql"

        else "PySpark"

    )


    full_prompt = (

        f"Selected engine: "
        f"{selected_language}\n\n"

        f"Generate only "
        f"{selected_language} code.\n\n"

        f"Request: "
        f"{prompt}"

    )


    try:


        response = (

            requests.post(


                ANTHROPIC_API_URL,


                headers={


                    "x-api-key":

                        api_key,


                    "anthropic-version":

                        "2023-06-01",


                    "content-type":

                        "application/json",


                },


                json={


                    "model":

                        MODEL,


                    "max_tokens":

                        600,


                    "system":

                        SYSTEM_PROMPT,


                    "messages": [

                        {

                            "role":

                                "user",

                            "content":

                                full_prompt,

                        }

                    ],

                },


                timeout=20,

            )

        )


        response.raise_for_status()


        response_data = (

            response.json()

        )


        response_text = "".join(


            block.get(

                "text",

                "",

            )


            for block

            in response_data.get(

                "content",

                [],

            )


            if block.get(

                "type"

            )

            == "text"


        )


        if not response_text.strip():


            raise RuntimeError(

                "The external provider "
                "returned an empty response."

            )


        return (

            _parse_response_text(

                response_text

            )

        )


    except Exception:


        fallback = (

            _fallback_suggestion(

                prompt,

                selected_engine,

            )

        )


        fallback.source = (

            f"local-{selected_engine}-template-"
            "fallback-after-api-unavailable"

        )


        return fallback