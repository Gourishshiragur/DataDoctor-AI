"""
Conversational AI — multi-turn chat with optional RAG context.

Features:
- Multi-turn conversation history
- Optional incident-memory RAG
- Anthropic API integration
- Streaming responses
- Automatic zero-cost local fallback
"""

import json

import requests
import streamlit as st

from core.ai_assistant import (
    _get_api_key,
    ANTHROPIC_API_URL,
    MODEL,
)

from core.rag_memory import (
    incident_count,
    retrieve_similar,
)

from core.ui import (
    inject_global_css,
    sidebar_brand,
)


inject_global_css()
sidebar_brand()


st.title("💬 Conversational AI")

st.caption(
    "Ask about Spark, PySpark, Delta Lake, SCD2, ADF, "
    "pipeline architecture, optimization, or paste an error "
    "for troubleshooting."
)


SYSTEM_PROMPT = """
You are DataDoctor AI, a senior data engineering assistant.

You specialize in:

- Azure Databricks
- PySpark
- Apache Spark
- Delta Lake
- Azure Data Factory
- Spark Structured Streaming
- Bronze-Silver-Gold architecture
- Data quality
- Pipeline debugging
- Spark optimization

When debugging:

1. Identify the likely root cause.
2. Explain why the issue occurred.
3. Provide practical PySpark, Python, or SQL code.
4. Recommend production-level improvements.

Explain architecture trade-offs clearly.

Keep responses concise but complete.
"""


def local_answer(question: str) -> str:
    """
    Provide local Data Engineering guidance when an external
    LLM is unavailable, not configured, or has no credits.
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

The DataFrame may not contain the expected `device_id`
column.

An earlier transformation may also have removed or renamed
the column.

Inspect the schema:

    df.printSchema()
    print(df.columns)

Validate the required column:

    required_column = "device_id"

    if required_column not in df.columns:
        raise ValueError(
            f"Missing required column: {required_column}. "
            f"Available columns: {df.columns}"
        )

If the source uses another column name:

    df = df.withColumnRenamed(
        "deviceId",
        "device_id",
    )

Also inspect earlier:

- select()
- join()
- groupBy()
- Aggregations
- Column aliases
- Rename operations

Likely root cause:

Source-schema mismatch or a column removed during an
earlier transformation.
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
3. Update effective_to.
4. Set is_current to false.
5. Insert the changed record as a new current version.

Example:

    from pyspark.sql.functions import (
        current_timestamp,
        lit,
    )

    new_versions = (
        source_df
        .withColumn(
            "effective_from",
            current_timestamp(),
        )
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp"),
        )
        .withColumn(
            "is_current",
            lit(True),
        )
    )

For production, combine this with:

- Delta Lake MERGE
- Business-key matching
- Change detection
- Duplicate validation
- Audit logging
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

    from pyspark.sql.functions import (
        col,
        row_number,
    )

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

This retains the newest event for every device_id.
"""


    if (
        "merge" in q
        or "upsert" in q
    ):
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
            (
                "target.device_id "
                "= source.device_id"
            ),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

For production idempotency:

- Deduplicate source records before MERGE.
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
- Broadcast small dimension tables.
- Reduce unnecessary shuffle operations.
- Repartition using useful join keys.
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

Bronze:

- Stores raw source data.
- Adds ingestion metadata.
- Keeps the original source values.
- Applies minimal transformations.

Silver:

- Schema validation
- Type conversion
- Deduplication
- Data-quality rules
- Standardization
- Business transformations

Gold:

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

Useful pipeline parameters:

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

Example:

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
- Make foreachBatch operations idempotent.
- Use a separate checkpoint for every stream.
"""


    if (
        "cost" in q
        or "free" in q
        or "credit" in q
        or "price" in q
    ):
        return """
### DataDoctor AI operating modes

DataDoctor AI supports a zero-cost demonstration mode for
its core platform features:

- Pipeline simulation
- Monitoring
- Retry and repair
- Structured logs
- Analytics
- Local diagnostic guidance
- Local code patterns
- Incident-memory retrieval

External generative AI providers are optional.

Live LLM responses may require API credits depending on
the configured provider.

If the external provider is unavailable or has insufficient
credits, DataDoctor AI automatically continues using its
local diagnostic engine.
"""


    return """
### DataDoctor AI — Local Diagnostic Mode

The external AI provider is currently unavailable.

DataDoctor AI continued using its zero-cost local
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

Try asking:

How do I implement SCD Type 2 using Delta Lake?

or:

How do I optimize a slow Spark join?
"""


if "chat_history" not in st.session_state:
    st.session_state.chat_history = []


with st.sidebar:

    st.divider()

    st.markdown("**Chat settings**")

    rag_enabled = st.toggle(
        "Use incident memory (RAG)",
        value=False,
        help=(
            "Search stored resolved incidents and add "
            "relevant operational context."
        ),
    )

    stream_enabled = st.toggle(
        "Stream live LLM responses",
        value=True,
    )

    if rag_enabled:

        st.caption(
            "📚 Incident memory will load only "
            "after you submit a question."
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
    "Ask about Spark, Delta Lake, SCD2, ADF, "
    "or paste an error..."
)


if user_input:

    st.session_state.chat_history.append(
        {
            "role": "user",
            "content": user_input,
        }
    )


    with st.chat_message("user"):

        st.markdown(
            user_input
        )


    rag_context = ""

    similar_incidents = []


    if rag_enabled:

        try:

            with st.spinner(
                "Searching incident memory..."
            ):

                memory_count = incident_count()


                if memory_count > 0:

                    similar_incidents = (
                        retrieve_similar(
                            user_input,
                            k=2,
                        )
                    )


            if similar_incidents:

                incident_parts = []


                for result in similar_incidents:

                    incident_text = (
                        "Past incident "
                        f"(similarity "
                        f"{result.similarity_score:.2f}):\n"
                        f"Error: "
                        f"{result.incident.error_message}\n"
                        f"Root cause: "
                        f"{result.incident.root_cause}\n"
                        f"Fix: "
                        f"{result.incident.fix_applied}"
                    )

                    incident_parts.append(
                        incident_text
                    )


                rag_context = (
                    "\n\n"
                    "Relevant resolved incidents "
                    "from DataDoctor memory:\n"
                    + "\n---\n".join(
                        incident_parts
                    )
                )


        except Exception:

            st.warning(
                "Incident memory is temporarily "
                "unavailable. Continuing without "
                "RAG context."
            )


    system_prompt = (
        SYSTEM_PROMPT
        + rag_context
    )


    api_key = _get_api_key()


    messages = [

        {
            "role": message["role"],
            "content": message["content"],
        }

        for message
        in st.session_state.chat_history

    ]


    try:

        if not api_key:

            raise RuntimeError(
                "External provider is not configured."
            )


        with st.chat_message("assistant"):


            if stream_enabled:

                api_response = requests.post(

                    ANTHROPIC_API_URL,

                    headers={
                        "x-api-key": api_key,
                        "anthropic-version":
                            "2023-06-01",
                        "content-type":
                            "application/json",
                    },

                    json={
                        "model": MODEL,
                        "max_tokens": 1000,
                        "system": system_prompt,
                        "messages": messages,
                        "stream": True,
                    },

                    timeout=60,

                    stream=True,

                )


                if not api_response.ok:

                    raise RuntimeError(
                        "External provider is unavailable."
                    )


                placeholder = st.empty()

                full_response = ""


                for raw_line in (
                    api_response.iter_lines()
                ):

                    if not raw_line:

                        continue


                    if isinstance(
                        raw_line,
                        bytes,
                    ):

                        line = raw_line.decode(
                            "utf-8"
                        )

                    else:

                        line = raw_line


                    if not line.startswith(
                        "data: "
                    ):

                        continue


                    payload = line[6:]


                    if (
                        payload.strip()
                        == "[DONE]"
                    ):

                        break


                    try:

                        event = json.loads(
                            payload
                        )

                    except json.JSONDecodeError:

                        continue


                    if (
                        event.get("type")
                        == "content_block_delta"
                    ):

                        delta = event.get(
                            "delta",
                            {},
                        )


                        if (
                            delta.get("type")
                            == "text_delta"
                        ):

                            full_response += (
                                delta.get(
                                    "text",
                                    "",
                                )
                            )


                            placeholder.markdown(
                                full_response
                                + "▌"
                            )


                if not full_response:

                    raise RuntimeError(
                        "The external provider returned "
                        "an empty response."
                    )


                placeholder.markdown(
                    full_response
                )


                response_text = (
                    full_response
                )


            else:

                api_response = requests.post(

                    ANTHROPIC_API_URL,

                    headers={
                        "x-api-key": api_key,
                        "anthropic-version":
                            "2023-06-01",
                        "content-type":
                            "application/json",
                    },

                    json={
                        "model": MODEL,
                        "max_tokens": 1000,
                        "system": system_prompt,
                        "messages": messages,
                    },

                    timeout=30,

                )


                if not api_response.ok:

                    raise RuntimeError(
                        "External provider is unavailable."
                    )


                response_data = (
                    api_response.json()
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

                    if (
                        block.get("type")
                        == "text"
                    )

                )


                if not response_text:

                    raise RuntimeError(
                        "The external provider returned "
                        "an empty response."
                    )


                st.markdown(
                    response_text
                )


            if rag_context:

                st.caption(
                    "📚 RAG context added from "
                    f"{len(similar_incidents)} "
                    "similar incident(s)."
                )


        st.session_state.chat_history.append(
            {
                "role": "assistant",
                "content": response_text,
            }
        )


    except Exception:

        response_text = local_answer(
            user_input
        )


        with st.chat_message(
            "assistant"
        ):

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