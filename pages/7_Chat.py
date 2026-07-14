"""
DataDoctor AI — Enterprise Conversational AI

Features:
- Multi-turn conversational AI
- Free local Ollama integration
- Automatic Ollama model detection
- Incident-memory RAG
- Enterprise document and dataset RAG
- Uploaded-data grounded answers
- Calculated analysis context
- Source-aware responses
- Zero-cost troubleshooting fallback
- Business-impact explanations
- Existing RAG controls preserved
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

import streamlit as st

from core.ai_assistant import (
    DEFAULT_OLLAMA_MODEL,
    DEFAULT_OLLAMA_URL,
    ask_data_doctor,
    ollama_health,
)
from core.rag_memory import (
    get_rag_context,
    incident_count,
    knowledge_count,
    retrieve_similar,
)
from core.ui import (
    inject_global_css,
    sidebar_brand,
)


# =========================================================
# PAGE UI
# =========================================================

inject_global_css()
sidebar_brand()

st.title("💬 Conversational AI")

st.caption(
    "Ask about Spark, PySpark, Delta Lake, SCD Type 2, "
    "Azure Data Factory, pipeline architecture, optimization, "
    "uploaded data, calculated business outcomes, or paste "
    "an error for troubleshooting."
)


# =========================================================
# ERROR EXTRACTION
# =========================================================

def extract_error_signal(
    question: str,
) -> str:
    """
    Extract a useful error indicator from pasted logs.
    """

    text = str(
        question
        or ""
    ).strip()

    patterns = [
        (
            r"([A-Za-z]+Exception)"
            r"\s*:\s*([^\n]+)"
        ),
        (
            r"([A-Za-z]+Error)"
            r"\s*:\s*([^\n]+)"
        ),
        (
            r"(cannot resolve[^\n]+)"
        ),
        (
            r"(unresolved column[^\n]+)"
        ),
        (
            r"(column[^\n]+"
            r"not found[^\n]*)"
        ),
        (
            r"(table or view "
            r"not found[^\n]*)"
        ),
        (
            r"(path does not "
            r"exist[^\n]*)"
        ),
    ]

    for pattern in patterns:

        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE,
        )

        if match:

            return (
                match
                .group(0)
                .strip()
            )

    if not text:

        return ""

    return (
        text
        .splitlines()[0][
            :180
        ]
    )


# =========================================================
# BUILT-IN FALLBACK KNOWLEDGE
# =========================================================

def troubleshooting_answer(
    question: str,
) -> str:
    """
    Generate useful zero-cost diagnostic guidance when the
    local Ollama endpoint is unavailable.

    This fallback answers supported questions instead of
    replacing every answer with Ollama installation steps.
    """

    original_question = str(
        question
        or ""
    ).strip()

    q = (
        original_question
        .lower()
    )

    error_signal = (
        extract_error_signal(
            original_question
        )
    )


    # -----------------------------------------------------
    # MISSING COLUMN
    # -----------------------------------------------------

    if (
        "analysisexception" in q
        or "cannot resolve" in q
        or "unresolved column" in q
        or "missing column" in q
        or "column not found" in q
        or "cannot find column" in q
    ):

        return f"""
### Diagnosis

The pipeline is referencing a column that is unavailable at
the failing stage.

**Detected context**

{error_signal}

### Likely root causes

- The source schema changed.
- The column was renamed.
- Column capitalization differs.
- An earlier select operation removed the field.
- A join changed the expected column reference.
- An aggregation removed non-grouped fields.
- The transformation is using the wrong DataFrame.

### Troubleshooting

Inspect the schema immediately before the failing operation.

Python example:

    df.printSchema()
    print(df.columns)

Validate required fields before processing.

    required_columns = [
        "device_id",
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Missing required columns: "
            f"{{missing_columns}}. "
            "Available columns: "
            f"{{df.columns}}"
        )

If the source uses another field name:

    df = (
        df
        .withColumnRenamed(
            "deviceId",
            "device_id",
        )
    )

Review every earlier:

- select
- drop
- withColumnRenamed
- join
- groupBy
- aggregation
- alias

### Recommended fix

Standardize source field names in the Silver layer and
validate the required schema before business transformations.

### Business outcome

Correct schema validation reduces failed runs, delayed
reporting, incomplete downstream data, and SLA risk.
"""


    # -----------------------------------------------------
    # NULL ERROR
    # -----------------------------------------------------

    if (
        "nullpointerexception" in q
        or "none type" in q
        or "nonetype" in q
        or "null value" in q
    ):

        return f"""
### Diagnosis

The error suggests that the pipeline attempted to use an
object or value that was null or was not initialized.

**Detected context**

{error_signal}

### Troubleshooting

Inspect the value before the failing operation.

    print(type(value))
    print(value)

For a PySpark DataFrame field:

    from pyspark.sql.functions import col

    null_count = (
        df
        .filter(
            col(
                "required_column"
            ).isNull()
        )
        .count()
    )

    print(
        "Null rows:",
        null_count,
    )

### Recommended fix

- Validate required configuration values.
- Validate DataFrames before transformation.
- Apply business-approved null-handling rules.
- Quarantine invalid records when auditability is required.
- Record rejected-row counts in an audit table.
- Avoid silently replacing critical business values.

### Business outcome

Controlled null handling improves data completeness and
prevents unexpected pipeline interruption.
"""


    # -----------------------------------------------------
    # MEMORY ERROR
    # -----------------------------------------------------

    if (
        "outofmemory" in q
        or "out of memory" in q
        or "java heap space" in q
        or "gc overhead" in q
    ):

        return """
### Diagnosis

The Spark workload is likely creating excessive memory
pressure on one or more executors or on the driver.

### Likely causes

- Large data collected to the driver
- Data skew
- Oversized partitions
- Large non-broadcast joins
- Excessive caching
- Wide transformations
- Too many records in one partition

### Troubleshooting

Inspect the execution plan.

    result_df.explain(
        "formatted"
    )

Inspect partition distribution.

    partition_counts = (
        df
        .rdd
        .mapPartitions(
            lambda rows: [
                sum(
                    1
                    for _ in rows
                )
            ]
        )
        .collect()
    )

    print(
        partition_counts
    )

### Recommended actions

- Remove unnecessary collect and toPandas operations.
- Filter records early.
- Select only required fields.
- Repartition using an appropriate key.
- Investigate skewed join keys.
- Broadcast only genuinely small lookup tables.
- Unpersist cached DataFrames after use.
- Review executor memory after optimizing the plan.

### Business outcome

Reducing memory pressure improves completion reliability,
reduces retry effort, and supports predictable SLA delivery.
"""


    # -----------------------------------------------------
    # FILE OR PATH ERROR
    # -----------------------------------------------------

    if (
        "file not found" in q
        or "path does not exist" in q
        or "filenotfounderror" in q
    ):

        return f"""
### Diagnosis

The pipeline cannot locate the configured source or target
path.

**Detected context**

{error_signal}

### Validate

- Storage account name
- Container name
- Environment-specific base path
- Date partition
- File name
- Mount configuration
- External location
- Service-principal permissions

Log the resolved path before reading.

    print(
        "Resolved path:",
        source_path,
    )

### Recommended fix

Use centralized environment configuration and fail early with
a clear validation message when a required path is missing.

### Business outcome

Reliable path validation prevents avoidable ingestion delays
and reduces manual investigation effort.
"""


    # -----------------------------------------------------
    # SCD TYPE 2
    # -----------------------------------------------------

    if (
        "scd2" in q
        or "scd type 2" in q
        or "slowly changing dimension" in q
    ):

        return """
### SCD Type 2

SCD Type 2 preserves historical dimension values instead of
overwriting the current record.

Typical fields:

    business_key
    effective_from
    effective_to
    is_current
    attribute_hash

When tracked attributes change:

1. Match the current target record using the business key.
2. Compare tracked attributes or an attribute hash.
3. Close the previous version.
4. Set effective_to.
5. Set is_current to false.
6. Insert the changed record as a new current version.

### Enterprise controls

- Deduplicate source records.
- Validate one current record per business key.
- Use Delta Lake transactions.
- Make reruns idempotent.
- Track inserted, updated, unchanged, and rejected counts.
- Store batch ID and ingestion timestamp.

### Business outcome

Historical tracking supports accurate point-in-time
reporting, auditing, customer-history analysis, and
reproducible business metrics.
"""


    # -----------------------------------------------------
    # DEDUPLICATION
    # -----------------------------------------------------

    if (
        "deduplicate" in q
        or "deduplication" in q
        or "duplicate" in q
        or "latest record" in q
    ):

        return """
### Latest-record deduplication

PySpark example:

    from pyspark.sql import Window

    from pyspark.sql.functions import (
        col,
        row_number,
    )

    window_spec = (
        Window
        .partitionBy(
            "device_id"
        )
        .orderBy(
            col(
                "event_timestamp"
            ).desc(),
            col(
                "ingestion_timestamp"
            ).desc(),
        )
    )

    latest_df = (
        df
        .withColumn(
            "_row_number",
            row_number().over(
                window_spec
            ),
        )
        .filter(
            col(
                "_row_number"
            )
            == 1
        )
        .drop(
            "_row_number"
        )
    )

The second ordering field provides deterministic results when
multiple records have the same event timestamp.

### Enterprise validation

- Count duplicates before removal.
- Confirm the correct business key.
- Preserve duplicate records when auditability is required.
- Record input, output, rejected, and duplicate counts.
- Validate rerun behavior.

### Business outcome

Deterministic deduplication prevents double counting and
improves trust in reporting and downstream analytics.
"""


    # -----------------------------------------------------
    # DELTA MERGE
    # -----------------------------------------------------

    if (
        "merge" in q
        or "upsert" in q
    ):

        return """
### Delta Lake MERGE upsert

PySpark example:

    from delta.tables import DeltaTable

    target = (
        DeltaTable
        .forPath(
            spark,
            target_path,
        )
    )

    (
        target
        .alias(
            "target"
        )
        .merge(
            source_df.alias(
                "source"
            ),
            (
                "target.device_id "
                "= source.device_id"
            ),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

### Production controls

- Deduplicate the source before MERGE.
- Use a stable business key.
- Validate the source schema.
- Track processed batch IDs.
- Capture inserted and updated counts.
- Validate source-to-target reconciliation.
- Make reruns idempotent.

### Business outcome

An idempotent MERGE prevents duplicate target records and
supports safe replay and recovery.
"""


    # -----------------------------------------------------
    # SPARK OPTIMIZATION
    # -----------------------------------------------------

    if (
        "optimize" in q
        or "optimization" in q
        or "performance" in q
        or "slow spark" in q
        or "shuffle" in q
    ):

        return """
### Spark performance assessment

Review these areas in order:

1. Filter records early.
2. Select only required fields.
3. Inspect the physical execution plan.
4. Reduce unnecessary shuffle operations.
5. Broadcast only genuinely small dimensions.
6. Repartition using useful join or write keys.
7. Investigate skewed keys.
8. Enable Adaptive Query Execution.
9. Avoid excessive small Delta files.
10. Cache only reused DataFrames.

Broadcast example:

    from pyspark.sql.functions import (
        broadcast,
    )

    result_df = (
        fact_df
        .join(
            broadcast(
                dimension_df
            ),
            "device_id",
            "left",
        )
    )

Inspect the execution plan:

    result_df.explain(
        "formatted"
    )

### Business outcome

Performance tuning can reduce processing delay, improve SLA
predictability, and reduce unnecessary compute usage. Exact
cost savings require measured runtime and infrastructure data.
"""


    # -----------------------------------------------------
    # MEDALLION ARCHITECTURE
    # -----------------------------------------------------

    if (
        "bronze" in q
        or "silver" in q
        or "gold" in q
        or "medallion" in q
    ):

        return """
### Medallion architecture

**Bronze**

- Raw source data
- Original source values
- Ingestion metadata
- Batch ID
- Source-file information
- Minimal transformation

**Silver**

- Schema validation
- Type conversion
- Deduplication
- Standardization
- Data-quality rules
- Business transformations
- Invalid-record handling

**Gold**

- Business KPIs
- Aggregations
- Reporting datasets
- Analytics-ready facts and dimensions

Typical flow:

    ADF, files, CDC, or events
                  |
                  v
            Bronze Delta
                  |
                  v
       Silver validated data
                  |
                  v
        Gold business models

### Business outcome

Layer separation improves traceability, replay capability,
data quality, governance, and confidence in reporting.
"""


    # -----------------------------------------------------
    # AZURE DATA FACTORY
    # -----------------------------------------------------

    if (
        "adf" in q
        or "azure data factory" in q
    ):

        return """
### Enterprise ADF orchestration

Typical flow:

    Storage event or schedule
                  |
                  v
          Lookup configuration
                  |
                  v
       ForEach customer or site
                  |
                  v
          Databricks workflow
                  |
                  v
         Audit and control tables

Useful parameters:

- customer_id
- site_id
- batch_date
- source_path
- run_id

Use control tables for:

- Watermarks
- Batch status
- Audit counts
- Replay
- Reprocessing
- Error tracking
- SLA monitoring

### Business outcome

Metadata-driven orchestration reduces duplicated pipeline
logic and improves scalability, supportability, and audit
visibility.
"""


    # -----------------------------------------------------
    # STRUCTURED STREAMING
    # -----------------------------------------------------

    if (
        "streaming" in q
        or "watermark" in q
        or "checkpoint" in q
        or "autoloader" in q
        or "auto loader" in q
    ):

        return """
### Spark Structured Streaming

PySpark example:

    stream_df = (
        spark
        .readStream
        .format(
            "cloudFiles"
        )
        .option(
            "cloudFiles.format",
            "json",
        )
        .load(
            source_path
        )
    )

    query = (
        stream_df
        .writeStream
        .format(
            "delta"
        )
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .outputMode(
            "append"
        )
        .start(
            target_path
        )
    )

### Production controls

- Keep checkpoint locations stable.
- Use explicit schemas.
- Handle late events using event-time watermarks.
- Make foreachBatch operations idempotent.
- Use a unique checkpoint for every stream.
- Monitor input rate, processing rate, batch duration, and
  state-store growth.

### Business outcome

Reliable streaming controls improve data freshness while
reducing duplicate processing and recovery risk.
"""


    # -----------------------------------------------------
    # MONITORING
    # -----------------------------------------------------

    if (
        "pipeline monitoring" in q
        or "monitor pipeline" in q
        or "observability" in q
        or "sla" in q
    ):

        return """
### Enterprise pipeline monitoring

Track:

- Run status
- Step status
- Start time
- Completion time
- End-to-end duration
- SLA threshold
- Input row count
- Output row count
- Rejected-row count
- Retry count
- Recovery time
- Data-quality score
- Source freshness
- Root-cause category

Recommended control tables:

    pipeline_run_audit
    pipeline_step_audit
    data_quality_results
    watermark_control
    incident_history

### Business outcome

Operational observability reduces diagnosis time, improves
SLA visibility, and helps teams identify recurring failures
before they affect downstream reporting.
"""


    # -----------------------------------------------------
    # DATA-SPECIFIC QUESTION WITHOUT VERIFIED CONTEXT
    # -----------------------------------------------------

    if (
        "salary" in q
        or "revenue" in q
        or "average" in q
        or "total" in q
        or "highest" in q
        or "lowest" in q
        or "uploaded data" in q
        or "dataset" in q
        or "csv" in q
    ):

        return """
### Verified dataset context is required

A numerical answer requires the uploaded dataset or calculated
analysis results to be available in the current application
session.

DataDoctor AI will not invent:

- Totals
- Averages
- Salaries
- Revenue values
- Percentages
- Trends
- Anomalies
- Business KPIs

Upload and process the real CSV through the application,
confirm that its profile and analysis results are available,
and then ask the question again.

When verified dataset context is available, the local Ollama
model can explain calculated values and business outcomes.
"""


    # -----------------------------------------------------
    # GENERAL FALLBACK
    # -----------------------------------------------------

    return f"""
### Assessment

Your question was:

> {original_question}

The free built-in diagnostic engine does not have enough
specific verified context to produce a reliable custom answer
for this request.

### Information required

Provide one or more of the following:

- Complete error message
- Relevant PySpark, SQL, or Python code
- Pipeline step that failed
- Source and target technology
- Expected result
- Actual result
- Input schema
- Pipeline configuration
- Relevant uploaded document
- Processed CSV dataset

### Available diagnostic areas

DataDoctor AI can provide built-in guidance for:

- PySpark and Spark errors
- Missing-column failures
- Null-related failures
- Memory and performance issues
- Delta Lake MERGE
- SCD Type 2
- Deduplication
- Azure Data Factory
- Medallion architecture
- Structured Streaming
- Pipeline monitoring
- Business and operational impact

The local Ollama provider is optional. When Ollama is
available on your laptop, DataDoctor AI uses it for flexible
natural-language responses. The built-in diagnostic mode
continues working without paid AI credits.
"""


# =========================================================
# SESSION STATE
# =========================================================

if (
    "chat_history"
    not in st.session_state
):

    st.session_state[
        "chat_history"
    ] = []


# =========================================================
# SIDEBAR SETTINGS
# =========================================================

with st.sidebar:

    st.divider()

    st.markdown(
        "**Chat settings**"
    )

    rag_enabled = st.toggle(
        "Use RAG memory",
        value=False,
        help=(
            "Search resolved incidents and indexed "
            "enterprise file and data context."
        ),
    )

    stream_enabled = st.toggle(
        "Stream live LLM responses",
        value=True,
        help=(
            "Keeps the existing UI option. Ollama currently "
            "returns the completed grounded response in one "
            "request."
        ),
    )

    ollama_status = (
        ollama_health()
    )

    installed_models = (
        ollama_status.get(
            "models",
            [],
        )
    )

    if installed_models:

        default_index = 0

        if (
            DEFAULT_OLLAMA_MODEL
            in installed_models
        ):

            default_index = (
                installed_models
                .index(
                    DEFAULT_OLLAMA_MODEL
                )
            )

        selected_model = (
            st.selectbox(
                "Local AI model",
                options=(
                    installed_models
                ),
                index=(
                    default_index
                ),
                help=(
                    "Installed Ollama models are free and "
                    "run locally on your laptop."
                ),
            )
        )

        st.success(
            "Ollama connected"
        )

        st.caption(
            "DataDoctor AI will use the selected free local "
            "model for flexible responses."
        )

    else:

        selected_model = (
            DEFAULT_OLLAMA_MODEL
        )

        st.info(
            "Built-in diagnostic mode active"
        )

        st.caption(
            "Ollama was not detected. Built-in enterprise "
            "troubleshooting remains available without paid "
            "AI credits."
        )

    if rag_enabled:

        st.caption(
            "RAG searches incident memory and indexed "
            "enterprise knowledge."
        )

        st.caption(
            f"Incident records: "
            f"{incident_count()} · "
            f"Knowledge chunks: "
            f"{knowledge_count()}"
        )

    else:

        st.caption(
            "RAG memory is currently off."
        )

    if st.button(
        "Clear chat",
        use_container_width=True,
    ):

        st.session_state[
            "chat_history"
        ] = []

        st.rerun()


# =========================================================
# DISPLAY CHAT HISTORY
# =========================================================

for message in (
    st.session_state[
        "chat_history"
    ]
):

    with st.chat_message(
        message[
            "role"
        ]
    ):

        st.markdown(
            message[
                "content"
            ]
        )


# =========================================================
# CHAT INPUT
# =========================================================

user_input = st.chat_input(
    "Ask about Spark, uploaded data, business outcomes, "
    "Delta Lake, SCD2, ADF, or paste an error..."
)


# =========================================================
# PROCESS USER QUESTION
# =========================================================

if user_input:

    previous_history = list(
        st.session_state[
            "chat_history"
        ]
    )

    st.session_state[
        "chat_history"
    ].append(
        {
            "role": "user",
            "content": user_input,
        }
    )

    with st.chat_message(
        "user"
    ):

        st.markdown(
            user_input
        )


    # -----------------------------------------------------
    # RAG CONTEXT
    # -----------------------------------------------------

    incident_context = ""

    enterprise_context = ""

    source_names: List[
        str
    ] = []

    similar_incidents = []

    enterprise_rag: Dict[
        str,
        Any,
    ] = {
        "retrieved": False,
        "context": "",
        "sources": [],
        "matches": [],
    }


    if rag_enabled:

        try:

            with st.spinner(
                "Searching RAG memory..."
            ):

                if (
                    incident_count()
                    > 0
                ):

                    similar_incidents = (
                        retrieve_similar(
                            user_input,
                            k=2,
                        )
                    )

                if (
                    knowledge_count()
                    > 0
                ):

                    enterprise_rag = (
                        get_rag_context(
                            query=(
                                user_input
                            ),
                            k=5,
                        )
                    )


            if similar_incidents:

                incident_parts = []

                for result in (
                    similar_incidents
                ):

                    incident_parts.append(
                        (
                            "Past resolved incident "
                            f"(similarity "
                            f"{result.similarity_score:.2f})\n"
                            f"Pipeline: "
                            f"{result.incident.pipeline_name}\n"
                            f"Error: "
                            f"{result.incident.error_message}\n"
                            f"Root cause: "
                            f"{result.incident.root_cause}\n"
                            f"Fix: "
                            f"{result.incident.fix_applied}"
                        )
                    )

                incident_context = (
                    "\n\n"
                    .join(
                        incident_parts
                    )
                )

                source_names.append(
                    "DataDoctor incident memory"
                )


            if enterprise_rag.get(
                "retrieved"
            ):

                enterprise_context = (
                    enterprise_rag.get(
                        "context",
                        "",
                    )
                )

                for source in (
                    enterprise_rag.get(
                        "sources",
                        [],
                    )
                ):

                    if (
                        source
                        not in source_names
                    ):

                        source_names.append(
                            source
                        )


        except Exception:

            st.warning(
                "RAG memory is temporarily unavailable. "
                "Continuing with local AI or built-in "
                "diagnostics."
            )


    combined_rag_context = (
        "\n\n"
        .join(
            context
            for context in [
                incident_context,
                enterprise_context,
            ]
            if context
        )
    )


    # -----------------------------------------------------
    # REAL DATASET AND ANALYSIS CONTEXT
    # -----------------------------------------------------

    dataset_context: Any = (
        st.session_state.get(
            "dataset_context"
        )
        or st.session_state.get(
            "data_profile"
        )
        or st.session_state.get(
            "dataset_profile"
        )
        or st.session_state.get(
            "uploaded_dataset_profile"
        )
    )

    analysis_context: Any = (
        st.session_state.get(
            "analysis_results"
        )
        or st.session_state.get(
            "latest_analysis"
        )
        or st.session_state.get(
            "quality_report"
        )
        or st.session_state.get(
            "data_quality_report"
        )
    )

    business_context: Any = (
        st.session_state.get(
            "business_context"
        )
        or st.session_state.get(
            "business_insights"
        )
        or st.session_state.get(
            "business_outcomes"
        )
    )


    # -----------------------------------------------------
    # GENERATE RESPONSE
    # -----------------------------------------------------

    with st.chat_message(
        "assistant"
    ):

        response_placeholder = None

        if stream_enabled:

            response_placeholder = (
                st.empty()
            )

            response_placeholder.markdown(
                "Analyzing verified context..."
            )


        with st.spinner(
            "DataDoctor AI is analyzing..."
        ):

            result = (
                ask_data_doctor(
                    question=(
                        user_input
                    ),
                    dataset_context=(
                        dataset_context
                    ),
                    rag_context=(
                        combined_rag_context
                        or None
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
                    model=(
                        selected_model
                    ),
                    base_url=(
                        DEFAULT_OLLAMA_URL
                    ),
                    chat_history=(
                        previous_history
                    ),
                    temperature=0.1,
                )
            )


        # -------------------------------------------------
        # OLLAMA RESPONSE
        # -------------------------------------------------

        if result.get(
            "success"
        ):

            response_text = str(
                result.get(
                    "answer",
                    "",
                )
            ).strip()

            if not response_text:

                response_text = (
                    "The local model returned an empty "
                    "response."
                )


            if response_placeholder:

                response_placeholder.markdown(
                    response_text
                )

            else:

                st.markdown(
                    response_text
                )


            st.caption(
                "Response provider: free local Ollama · "
                f"Model: {selected_model}"
            )


            if source_names:

                st.caption(
                    "Grounded with RAG context from: "
                    + ", ".join(
                        source_names
                    )
                )


            if (
                dataset_context
                or analysis_context
            ):

                st.caption(
                    "Verified uploaded-data or calculated "
                    "analysis context was supplied to the "
                    "response."
                )


        # -------------------------------------------------
        # BUILT-IN FALLBACK
        # -------------------------------------------------

        else:

            response_text = (
                troubleshooting_answer(
                    user_input
                )
            )


            if response_placeholder:

                response_placeholder.markdown(
                    response_text
                )

            else:

                st.markdown(
                    response_text
                )


            st.caption(
                "Response provider: DataDoctor AI "
                "zero-cost built-in diagnostic engine"
            )


            if source_names:

                st.caption(
                    "Retrieved RAG context available from: "
                    + ", ".join(
                        source_names
                    )
                )


            if (
                dataset_context
                or analysis_context
            ):

                st.caption(
                    "Dataset context is available, but the "
                    "local Ollama provider was unavailable. "
                    "No unsupported numerical answer was "
                    "generated."
                )


    # -----------------------------------------------------
    # SAVE ASSISTANT RESPONSE
    # -----------------------------------------------------

    st.session_state[
        "chat_history"
    ].append(
        {
            "role": "assistant",
            "content": (
                response_text
            ),
        }
    )