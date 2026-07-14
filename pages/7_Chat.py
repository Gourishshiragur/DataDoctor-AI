"""
DataDoctor AI — Enterprise Conversational AI

Features:
- Multi-turn conversation history
- Free local Ollama integration
- Incident-memory RAG
- Enterprise document and dataset RAG
- Uploaded-data grounded answers
- Source-aware responses
- Automatic zero-cost local diagnostic fallback
- Existing UI controls preserved
"""

from __future__ import annotations

from typing import Any, List

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
from core.ui import inject_global_css, sidebar_brand


inject_global_css()
sidebar_brand()

st.title("💬 Conversational AI")

st.caption(
    "Ask about Spark, PySpark, Delta Lake, SCD2, ADF, "
    "pipeline architecture, optimization, uploaded data, "
    "business outcomes, or paste an error for troubleshooting."
)


def local_answer(question: str) -> str:
    """
    Provide useful zero-cost guidance when Ollama is unavailable.
    """

    q = question.lower()

    if (
        "analysisexception" in q
        or "cannot resolve" in q
        or "unresolved column" in q
        or "missing column" in q
        or "device_id" in q
    ):
        return """
### Diagnosis

The DataFrame may not contain the expected `device_id` column,
or an earlier transformation may have removed or renamed it.

Inspect the schema:

    df.printSchema()
    print(df.columns)

Validate required columns:

    required_columns = ["device_id"]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}. "
            f"Available columns: {df.columns}"
        )

If the source uses another name:

    df = df.withColumnRenamed(
        "deviceId",
        "device_id",
    )

Check earlier `select()`, `join()`, `groupBy()`, aggregation,
alias, and rename operations.

**Likely root cause:** source-schema mismatch or a required
column removed during an earlier transformation.

**Business impact:** the pipeline may fail before producing
validated downstream data, affecting reporting freshness and
SLA completion.
"""

    if (
        "scd2" in q
        or "scd type 2" in q
        or "slowly changing dimension" in q
    ):
        return """
### SCD Type 2

SCD Type 2 preserves historical versions instead of
overwriting existing dimension records.

Typical columns:

    customer_id
    effective_from
    effective_to
    is_current

When an attribute changes:

1. Match using the business key.
2. Close the existing current record.
3. Update `effective_to`.
4. Set `is_current` to `false`.
5. Insert the changed record as a new current version.

For production, combine this with:

- Delta Lake `MERGE`
- Business-key matching
- Hash-based change detection
- Source deduplication
- Audit logging
- Idempotent reruns
"""

    if (
        "deduplicate" in q
        or "deduplication" in q
        or "duplicate" in q
        or "latest record" in q
    ):
        return """
### Deduplicate and retain the latest event

    from pyspark.sql import Window
    from pyspark.sql.functions import col, row_number

    window_spec = (
        Window
        .partitionBy("device_id")
        .orderBy(
            col("event_timestamp").desc()
        )
    )

    latest_df = (
        df
        .withColumn(
            "row_number",
            row_number().over(window_spec),
        )
        .filter(
            col("row_number") == 1
        )
        .drop("row_number")
    )

This retains the newest event for every `device_id`.

For deterministic results, add a second ordering column such
as ingestion timestamp or sequence number.
"""

    if "merge" in q or "upsert" in q:
        return """
### Delta Lake MERGE upsert

    from delta.tables import DeltaTable

    target = DeltaTable.forPath(
        spark,
        target_path,
    )

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            "target.device_id = source.device_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

For production idempotency:

- Deduplicate source records before `MERGE`.
- Use a stable business key.
- Track processed batch IDs.
- Validate source and target counts.
- Write results to an audit table.
"""

    if (
        "optimize" in q
        or "optimization" in q
        or "performance" in q
        or "slow spark" in q
        or "shuffle" in q
    ):
        return """
### Spark optimization checklist

Review these areas:

- Filter records before joins.
- Select only required columns.
- Broadcast genuinely small dimension tables.
- Reduce unnecessary shuffle operations.
- Repartition using useful join or write keys.
- Enable Adaptive Query Execution.
- Avoid excessive small Delta files.
- Use partition pruning.
- Cache only reused DataFrames.

Broadcast example:

    from pyspark.sql.functions import broadcast

    result_df = (
        fact_df
        .join(
            broadcast(dimension_df),
            "device_id",
            "left",
        )
    )

Inspect the execution plan:

    result_df.explain("formatted")
"""

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
- Ingestion metadata
- Original source values
- Minimal transformation

**Silver**

- Schema validation
- Type conversion
- Deduplication
- Data-quality rules
- Standardization
- Business transformations

**Gold**

- KPIs
- Aggregations
- Reporting datasets
- Analytics-ready business tables

Typical flow:

    ADF or Event Source
             ↓
        Bronze Delta
             ↓
    Silver Validated Data
             ↓
     Gold Business Metrics
"""

    if (
        "adf" in q
        or "azure data factory" in q
    ):
        return """
### Enterprise ADF orchestration

Typical flow:

    Storage Event Trigger
              ↓
    Lookup Configuration
              ↓
    ForEach Customer or Site
              ↓
    Databricks Notebook
              ↓
    Audit and Control Tables

Useful parameters:

- `customer_id`
- `site_id`
- `batch_date`
- `source_path`
- `run_id`

Use control tables for:

- Watermarks
- Batch status
- Audit counts
- Replay
- Reprocessing
- Error tracking
- SLA monitoring
"""

    if (
        "streaming" in q
        or "watermark" in q
        or "checkpoint" in q
        or "autoloader" in q
        or "auto loader" in q
    ):
        return """
### Spark Structured Streaming

    stream_df = (
        spark
        .readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "json",
        )
        .load(source_path)
    )

    query = (
        stream_df
        .writeStream
        .format("delta")
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .outputMode("append")
        .start(target_path)
    )

For production:

- Keep checkpoint locations stable.
- Use explicit schemas.
- Handle late events with watermarks.
- Make `foreachBatch` operations idempotent.
- Use a separate checkpoint for every stream.
"""

    if (
        "salary" in q
        or "revenue" in q
        or "average" in q
        or "total" in q
        or "highest" in q
        or "lowest" in q
        or "uploaded data" in q
        or "dataset" in q
    ):
        return """
### Uploaded-data answer unavailable

A verified numerical answer requires the uploaded dataset or
a calculated analysis result to be available in the current
application session.

DataDoctor AI will not invent totals, averages, salaries,
revenue values, trends, or business KPIs.

Upload or analyze the dataset first, then ask the question
again with RAG enabled.
"""

    return """
### DataDoctor AI — Local Diagnostic Mode

The local Ollama model is currently unavailable, so
DataDoctor AI continued using its zero-cost built-in
knowledge engine.

Built-in guidance is available for:

- PySpark errors
- Missing-column problems
- Delta Lake MERGE
- SCD Type 2
- Deduplication
- Spark optimization
- Azure Data Factory
- Medallion architecture
- Structured Streaming
- Pipeline monitoring

For unrestricted local AI responses:

1. Install Ollama.
2. Run:

    ollama pull llama3.2:3b

3. Start Ollama.
4. Select the installed model in Chat settings.
"""


if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


with st.sidebar:
    st.divider()

    st.markdown("**Chat settings**")

    rag_enabled = st.toggle(
        "Use RAG memory",
        value=False,
        help=(
            "Search resolved incidents and indexed "
            "enterprise file/data context."
        ),
    )

    stream_enabled = st.toggle(
        "Stream live LLM responses",
        value=True,
        help=(
            "Keeps the existing UI option. Local Ollama "
            "currently returns the completed grounded "
            "response in one request."
        ),
    )

    ollama_status = ollama_health()

    installed_models = ollama_status.get(
        "models",
        [],
    )

    if installed_models:
        default_index = 0

        if DEFAULT_OLLAMA_MODEL in installed_models:
            default_index = installed_models.index(
                DEFAULT_OLLAMA_MODEL
            )

        selected_model = st.selectbox(
            "Local AI model",
            options=installed_models,
            index=default_index,
            help=(
                "Installed Ollama models are free and "
                "run on your machine."
            ),
        )

        st.caption("🟢 Ollama connected")

    else:
        selected_model = DEFAULT_OLLAMA_MODEL

        st.caption(
            "🟡 Ollama is not connected. "
            "Built-in local diagnostic mode remains "
            "available."
        )

    if rag_enabled:
        st.caption(
            "📚 RAG will search incident memory and "
            "indexed enterprise knowledge after you "
            "submit a question."
        )

        st.caption(
            f"Incident records: {incident_count()} · "
            f"Knowledge chunks: {knowledge_count()}"
        )

    else:
        st.caption(
            "🟢 Zero-cost local mode is available."
        )

    if st.button(
        "Clear chat",
        use_container_width=True,
    ):
        st.session_state.chat_history = []

        st.rerun()


for message in st.session_state.chat_history:
    with st.chat_message(
        message["role"]
    ):
        st.markdown(
            message["content"]
        )


user_input = st.chat_input(
    "Ask about Spark, uploaded data, business outcomes, "
    "Delta Lake, SCD2, ADF, or paste an error..."
)


if user_input:
    previous_history = list(
        st.session_state.chat_history
    )

    st.session_state.chat_history.append(
        {
            "role": "user",
            "content": user_input,
        }
    )

    with st.chat_message("user"):
        st.markdown(user_input)

    incident_context = ""

    enterprise_context = ""

    source_names: List[str] = []

    similar_incidents = []

    enterprise_rag = {
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
                if incident_count() > 0:
                    similar_incidents = (
                        retrieve_similar(
                            user_input,
                            k=2,
                        )
                    )

                if knowledge_count() > 0:
                    enterprise_rag = (
                        get_rag_context(
                            query=user_input,
                            k=5,
                        )
                    )

            if similar_incidents:
                incident_parts = []

                for result in similar_incidents:
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

                incident_context = "\n\n".join(
                    incident_parts
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

                for source in enterprise_rag.get(
                    "sources",
                    [],
                ):
                    if source not in source_names:
                        source_names.append(
                            source
                        )

        except Exception:
            st.warning(
                "RAG memory is temporarily unavailable. "
                "Continuing without retrieved context."
            )

    combined_rag_context = "\n\n".join(
        context
        for context in [
            incident_context,
            enterprise_context,
        ]
        if context
    )

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
    )

    business_context: Any = (
        st.session_state.get(
            "business_context"
        )
        or st.session_state.get(
            "business_insights"
        )
    )

    with st.chat_message(
        "assistant"
    ):
        response_placeholder = None

        if stream_enabled:
            response_placeholder = st.empty()

            response_placeholder.markdown(
                "Analyzing verified context..."
            )

        with st.spinner(
            "DataDoctor AI is analyzing..."
        ):
            result = ask_data_doctor(
                question=user_input,
                dataset_context=dataset_context,
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
                source_names=source_names,
                model=selected_model,
                base_url=DEFAULT_OLLAMA_URL,
                chat_history=previous_history,
                temperature=0.1,
            )

        if result.get("success"):
            response_text = result.get(
                "answer",
                "",
            )

            if response_placeholder:
                response_placeholder.markdown(
                    response_text
                )

            else:
                st.markdown(
                    response_text
                )

            if source_names:
                st.caption(
                    "📚 Grounded with RAG context from: "
                    + ", ".join(
                        source_names
                    )
                )

        else:
            response_text = local_answer(
                user_input
            )

            if response_placeholder:
                response_placeholder.empty()

            st.caption(
                "🟢 DataDoctor local diagnostic mode"
            )

            st.markdown(
                response_text
            )

    st.session_state.chat_history.append(
        {
            "role": "assistant",
            "content": response_text,
        }
    )