"""
AI Code Assistant page.

Supports:
  - PySpark code generation
  - SQL code generation
  - Anthropic API when available
  - Zero-cost local template fallback
  - Engine-aware common patterns
"""

import streamlit as st

from core.ai_assistant import get_code_suggestion
from core.ui import inject_global_css, sidebar_brand


# ---------------------------------------------------------
# PAGE UI
# ---------------------------------------------------------

inject_global_css()
sidebar_brand()

st.title("🤖 AI Code Assistant")

st.caption(
    "Describe a data-engineering requirement in plain language "
    "and generate a production-oriented PySpark or SQL snippet."
)


# ---------------------------------------------------------
# HELPER: DISPLAY GENERATED SUGGESTION
# ---------------------------------------------------------

def display_suggestion(
    suggestion,
    selected_engine: str,
):
    """
    Display explanation, generated code, and generation source.
    """

    normalized_engine = (
        str(selected_engine)
        .strip()
        .lower()
    )

    language = (
        "sql"
        if normalized_engine == "sql"
        else "python"
    )

    code = suggestion.code.strip()

    # Remove Markdown code fences because st.code()
    # adds its own formatted code container.
    if code.startswith("```"):

        code_lines = code.splitlines()

        if code_lines and code_lines[0].startswith("```"):
            code_lines = code_lines[1:]

        if code_lines and code_lines[-1].strip() == "```":
            code_lines = code_lines[:-1]

        code = "\n".join(code_lines).strip()


    st.markdown(
        f"**Explanation:** {suggestion.explanation}"
    )

    st.code(
        code,
        language=language,
    )


    if suggestion.source == "claude-api":

        st.caption(
            "✅ Generated live using the configured "
            "external AI provider."
        )

    elif "fallback-after-api" in suggestion.source:

        st.caption(
            "🟢 The external AI provider is unavailable or "
            "has insufficient credits. DataDoctor AI "
            f"automatically generated this {normalized_engine.upper()} "
            "solution using its zero-cost local template engine."
        )

    else:

        st.caption(
            "🟢 Generated using the DataDoctor AI zero-cost "
            f"local {normalized_engine.upper()} template engine."
        )


# ---------------------------------------------------------
# CODE GENERATION FORM
# ---------------------------------------------------------

with st.form(
    "suggestion_form"
):

    engine = st.radio(
        "Code engine",
        options=[
            "pyspark",
            "sql",
        ],
        format_func=lambda value: (
            "PySpark"
            if value == "pyspark"
            else "SQL"
        ),
        horizontal=True,
        help=(
            "The selected engine controls both live AI "
            "generation and the zero-cost local fallback."
        ),
    )


    prompt = st.text_area(
        "What do you need?",
        placeholder=(
            "Example: Deduplicate incoming telemetry records "
            "and retain the latest record by event_time"
        ),
        height=100,
    )


    submitted = st.form_submit_button(
        "Generate suggestion",
        type="primary",
        use_container_width=True,
    )


# ---------------------------------------------------------
# GENERATE CUSTOM SUGGESTION
# ---------------------------------------------------------

if submitted:

    if not prompt.strip():

        st.error(
            "Describe the required transformation or "
            "data-engineering pattern first."
        )

    else:

        with st.spinner(
            f"Generating {engine.upper()} suggestion..."
        ):

            suggestion = get_code_suggestion(
                prompt=prompt.strip(),
                engine=engine,
            )


        st.subheader(
            "Generated solution"
        )


        display_suggestion(
            suggestion,
            engine,
        )


# ---------------------------------------------------------
# COMMON PATTERNS
# ---------------------------------------------------------

st.divider()

st.subheader(
    "Common patterns"
)

st.caption(
    "Choose the output engine below, then select a pattern. "
    "The generated code will respect the selected engine."
)


example_engine = st.radio(
    "Common-pattern engine",
    options=[
        "pyspark",
        "sql",
    ],
    format_func=lambda value: (
        "PySpark"
        if value == "pyspark"
        else "SQL"
    ),
    horizontal=True,
    key="common_pattern_engine",
)


examples = [
    {
        "label": "Incremental watermark load",
        "prompt": (
            "incremental load using a watermark column"
        ),
    },
    {
        "label": "SCD Type 2",
        "prompt": (
            "SCD2 merge to close the current version "
            "and open a new version when attributes change"
        ),
    },
    {
        "label": "Latest-record deduplication",
        "prompt": (
            "deduplicate on a natural key and retain "
            "the latest record by event time"
        ),
    },
    {
        "label": "Delta MERGE upsert",
        "prompt": (
            "Delta MERGE upsert using device_id "
            "and event_date"
        ),
    },
]


example_columns = st.columns(
    2
)


for index, example in enumerate(
    examples
):

    selected = example_columns[
        index % 2
    ].button(
        example["label"],
        key=(
            f"example-"
            f"{example_engine}-"
            f"{index}"
        ),
        use_container_width=True,
    )


    if selected:

        with st.spinner(
            f"Generating {example_engine.upper()} pattern..."
        ):

            example_suggestion = (
                get_code_suggestion(
                    prompt=example["prompt"],
                    engine=example_engine,
                )
            )


        st.markdown(
            f"### {example['label']}"
        )


        display_suggestion(
            example_suggestion,
            example_engine,
        )