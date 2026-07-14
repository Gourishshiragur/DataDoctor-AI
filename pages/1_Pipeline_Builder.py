"""
DataDoctor AI — Enterprise Pipeline Builder

Features:
- Existing template-based pipeline builder
- Existing manual source → transform → sink builder
- Real CSV, Excel, JSON, and Parquet ingestion
- File preview and schema detection
- Data-quality profiling
- Missing-value and duplicate analysis
- Automatic Bronze → Silver → Gold pipeline generation
- Verified business metrics from uploaded data
- Dataset context for Conversational AI
- Existing pipeline save and execution flow preserved
"""

from __future__ import annotations

import hashlib
import io
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

from core import store
from core.models import Pipeline, Step, new_id
from core.pipeline_engine import run_pipeline
from core.templates import get_templates
from core.ui import inject_global_css, sidebar_brand


# =========================================================
# PAGE UI
# =========================================================

inject_global_css()
sidebar_brand()

st.title("🛠️ Pipeline Builder")

st.caption(
    "Compose reusable source → transform → sink pipelines, "
    "or ingest a real enterprise file and automatically "
    "generate a profiled Bronze → Silver → Gold workflow."
)


# =========================================================
# SESSION STATE
# =========================================================

if "draft_steps" not in st.session_state:
    st.session_state.draft_steps = []

if "uploaded_dataframe" not in st.session_state:
    st.session_state.uploaded_dataframe = None

if "uploaded_file_name" not in st.session_state:
    st.session_state.uploaded_file_name = None

if "uploaded_file_hash" not in st.session_state:
    st.session_state.uploaded_file_hash = None

if "dataset_context" not in st.session_state:
    st.session_state.dataset_context = None

if "data_profile" not in st.session_state:
    st.session_state.data_profile = None

if "business_context" not in st.session_state:
    st.session_state.business_context = None

if "business_outcomes" not in st.session_state:
    st.session_state.business_outcomes = None

if "uploaded_dataset_profile" not in st.session_state:
    st.session_state.uploaded_dataset_profile = None


# =========================================================
# FILE-READING HELPERS
# =========================================================

def normalize_column_name(
    column_name: Any,
) -> str:
    """
    Standardize field names for reliable downstream use.
    """

    normalized = (
        str(column_name)
        .strip()
        .lower()
    )

    normalized = (
        normalized
        .replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("\\", "_")
        .replace(".", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("%", "percentage")
    )

    while "__" in normalized:
        normalized = normalized.replace(
            "__",
            "_",
        )

    return (
        normalized.strip("_")
        or "unnamed_column"
    )


def make_unique_columns(
    columns: List[Any],
) -> List[str]:
    """
    Normalize columns and make duplicate names unique.
    """

    used: Dict[str, int] = {}

    output: List[str] = []

    for column in columns:

        base_name = normalize_column_name(
            column
        )

        count = used.get(
            base_name,
            0,
        )

        if count == 0:

            final_name = base_name

        else:

            final_name = (
                f"{base_name}_{count + 1}"
            )

        used[
            base_name
        ] = count + 1

        output.append(
            final_name
        )

    return output


def read_uploaded_file(
    uploaded_file,
) -> pd.DataFrame:
    """
    Read a supported enterprise file into a real DataFrame.
    """

    file_name = (
        uploaded_file.name
        .lower()
        .strip()
    )

    file_bytes = (
        uploaded_file.getvalue()
    )

    buffer = io.BytesIO(
        file_bytes
    )

    if file_name.endswith(
        ".csv"
    ):

        try:

            dataframe = pd.read_csv(
                buffer
            )

        except UnicodeDecodeError:

            buffer.seek(0)

            dataframe = pd.read_csv(
                buffer,
                encoding="latin-1",
            )

    elif file_name.endswith(
        (
            ".xlsx",
            ".xls",
        )
    ):

        dataframe = pd.read_excel(
            buffer
        )

    elif file_name.endswith(
        ".json"
    ):

        try:

            dataframe = pd.read_json(
                buffer
            )

        except ValueError:

            buffer.seek(0)

            dataframe = pd.read_json(
                buffer,
                lines=True,
            )

    elif file_name.endswith(
        ".parquet"
    ):

        dataframe = pd.read_parquet(
            buffer
        )

    else:

        raise ValueError(
            "Unsupported file type. Upload CSV, Excel, "
            "JSON, or Parquet."
        )

    if dataframe is None:

        raise ValueError(
            "The uploaded file could not be converted "
            "into a dataset."
        )

    dataframe.columns = (
        make_unique_columns(
            list(
                dataframe.columns
            )
        )
    )

    return dataframe


# =========================================================
# DATA-PROFILING HELPERS
# =========================================================

def safe_python_value(
    value: Any,
) -> Any:
    """
    Convert pandas and NumPy values into JSON-safe values.
    """

    if pd.isna(
        value
    ):

        return None

    if hasattr(
        value,
        "item",
    ):

        try:

            return value.item()

        except Exception:

            pass

    if isinstance(
        value,
        pd.Timestamp,
    ):

        return value.isoformat()

    return value


def dataframe_sample(
    dataframe: pd.DataFrame,
    limit: int = 20,
) -> List[dict]:
    """
    Create a JSON-safe sample for local LLM grounding.
    """

    sample = (
        dataframe
        .head(
            limit
        )
        .copy()
    )

    rows: List[dict] = []

    for record in sample.to_dict(
        orient="records"
    ):

        rows.append(
            {
                str(key): (
                    safe_python_value(
                        value
                    )
                )
                for key, value
                in record.items()
            }
        )

    return rows


def numeric_summary(
    dataframe: pd.DataFrame,
) -> Dict[str, dict]:
    """
    Calculate verified numeric statistics.
    """

    output: Dict[
        str,
        dict,
    ] = {}

    numeric_columns = list(
        dataframe
        .select_dtypes(
            include="number"
        )
        .columns
    )

    for column in numeric_columns:

        series = pd.to_numeric(
            dataframe[
                column
            ],
            errors="coerce",
        )

        valid_series = (
            series.dropna()
        )

        if valid_series.empty:

            continue

        output[
            column
        ] = {
            "count": int(
                valid_series.count()
            ),
            "sum": float(
                valid_series.sum()
            ),
            "average": float(
                valid_series.mean()
            ),
            "minimum": float(
                valid_series.min()
            ),
            "maximum": float(
                valid_series.max()
            ),
            "median": float(
                valid_series.median()
            ),
        }

    return output


def categorical_summary(
    dataframe: pd.DataFrame,
    max_columns: int = 20,
) -> Dict[str, dict]:
    """
    Create verified category distributions.
    """

    output: Dict[
        str,
        dict,
    ] = {}

    candidate_columns = list(
        dataframe
        .select_dtypes(
            exclude="number"
        )
        .columns
    )

    for column in (
        candidate_columns[
            :max_columns
        ]
    ):

        value_counts = (
            dataframe[
                column
            ]
            .astype(
                "string"
            )
            .fillna(
                "<NULL>"
            )
            .value_counts(
                dropna=False
            )
            .head(
                15
            )
        )

        output[
            column
        ] = {
            str(
                key
            ): int(
                value
            )
            for key, value
            in value_counts.items()
        }

    return output


def detect_business_metrics(
    dataframe: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Calculate verified business KPIs when suitable columns
    exist.

    No values are invented.
    """

    metrics: Dict[
        str,
        Any,
    ] = {}

    columns = set(
        dataframe.columns
    )

    sales_candidates = [
        "total_sales",
        "sales",
        "revenue",
        "total_revenue",
        "net_sales",
        "sales_amount",
    ]

    cost_candidates = [
        "total_cost",
        "cost",
        "cost_amount",
        "expense",
        "expenses",
    ]

    profit_candidates = [
        "profit",
        "total_profit",
        "net_profit",
        "profit_amount",
    ]

    quantity_candidates = [
        "quantity",
        "qty",
        "units",
        "units_sold",
    ]

    def first_existing(
        candidates: List[str],
    ) -> Optional[str]:

        for candidate in candidates:

            if candidate in columns:

                return candidate

        return None

    sales_column = (
        first_existing(
            sales_candidates
        )
    )

    cost_column = (
        first_existing(
            cost_candidates
        )
    )

    profit_column = (
        first_existing(
            profit_candidates
        )
    )

    quantity_column = (
        first_existing(
            quantity_candidates
        )
    )

    if sales_column:

        total_sales = float(
            pd.to_numeric(
                dataframe[
                    sales_column
                ],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )

        metrics[
            "sales_column"
        ] = sales_column

        metrics[
            "total_sales"
        ] = total_sales

    if cost_column:

        total_cost = float(
            pd.to_numeric(
                dataframe[
                    cost_column
                ],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )

        metrics[
            "cost_column"
        ] = cost_column

        metrics[
            "total_cost"
        ] = total_cost

    if profit_column:

        total_profit = float(
            pd.to_numeric(
                dataframe[
                    profit_column
                ],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )

        metrics[
            "profit_column"
        ] = profit_column

        metrics[
            "total_profit"
        ] = total_profit

    elif (
        sales_column
        and cost_column
    ):

        total_profit = (
            metrics[
                "total_sales"
            ]
            - metrics[
                "total_cost"
            ]
        )

        metrics[
            "profit_column"
        ] = (
            "calculated_from_sales_minus_cost"
        )

        metrics[
            "total_profit"
        ] = float(
            total_profit
        )

    if (
        "total_profit"
        in metrics
        and metrics.get(
            "total_sales",
            0,
        )
        != 0
    ):

        metrics[
            "profit_rate_percentage"
        ] = round(
            (
                metrics[
                    "total_profit"
                ]
                / metrics[
                    "total_sales"
                ]
            )
            * 100,
            4,
        )

    if quantity_column:

        metrics[
            "quantity_column"
        ] = quantity_column

        metrics[
            "total_quantity"
        ] = float(
            pd.to_numeric(
                dataframe[
                    quantity_column
                ],
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )

    if (
        profit_column
        and "region"
        in columns
    ):

        regional_profit = (
            dataframe
            .assign(
                _verified_profit=(
                    pd.to_numeric(
                        dataframe[
                            profit_column
                        ],
                        errors="coerce",
                    )
                    .fillna(0)
                )
            )
            .groupby(
                "region",
                dropna=False,
            )[
                "_verified_profit"
            ]
            .sum()
            .sort_values(
                ascending=False
            )
        )

        if not regional_profit.empty:

            metrics[
                "profit_by_region"
            ] = {
                str(
                    key
                ): float(
                    value
                )
                for key, value
                in regional_profit.items()
            }

            metrics[
                "highest_profit_region"
            ] = {
                "region": str(
                    regional_profit.index[
                        0
                    ]
                ),
                "profit": float(
                    regional_profit.iloc[
                        0
                    ]
                ),
            }

    if (
        profit_column
        and "product"
        in columns
    ):

        product_profit = (
            dataframe
            .assign(
                _verified_profit=(
                    pd.to_numeric(
                        dataframe[
                            profit_column
                        ],
                        errors="coerce",
                    )
                    .fillna(0)
                )
            )
            .groupby(
                "product",
                dropna=False,
            )[
                "_verified_profit"
            ]
            .sum()
            .sort_values(
                ascending=False
            )
        )

        if not product_profit.empty:

            metrics[
                "profit_by_product"
            ] = {
                str(
                    key
                ): float(
                    value
                )
                for key, value
                in product_profit.items()
            }

            metrics[
                "highest_profit_product"
            ] = {
                "product": str(
                    product_profit.index[
                        0
                    ]
                ),
                "profit": float(
                    product_profit.iloc[
                        0
                    ]
                ),
            }

    return metrics


def build_profile(
    dataframe: pd.DataFrame,
    file_name: str,
    file_hash: str,
) -> Dict[str, Any]:
    """
    Build a verified enterprise dataset profile.
    """

    row_count = int(
        len(
            dataframe
        )
    )

    column_count = int(
        len(
            dataframe.columns
        )
    )

    duplicate_rows = int(
        dataframe
        .duplicated()
        .sum()
    )

    null_counts = {
        str(
            column
        ): int(
            dataframe[
                column
            ]
            .isna()
            .sum()
        )
        for column in dataframe.columns
    }

    total_cells = (
        row_count
        * column_count
    )

    missing_cells = int(
        sum(
            null_counts.values()
        )
    )

    completeness = (
        (
            1
            - (
                missing_cells
                / total_cells
            )
        )
        * 100
        if total_cells
        else 0.0
    )

    uniqueness = (
        (
            1
            - (
                duplicate_rows
                / row_count
            )
        )
        * 100
        if row_count
        else 0.0
    )

    quality_score = round(
        (
            completeness
            * 0.7
        )
        + (
            uniqueness
            * 0.3
        ),
        2,
    )

    return {
        "file_name": (
            file_name
        ),
        "file_hash": (
            file_hash
        ),
        "row_count": (
            row_count
        ),
        "column_count": (
            column_count
        ),
        "columns": [
            {
                "name": str(
                    column
                ),
                "data_type": str(
                    dataframe[
                        column
                    ].dtype
                ),
                "null_count": (
                    null_counts[
                        column
                    ]
                ),
                "non_null_count": int(
                    dataframe[
                        column
                    ]
                    .notna()
                    .sum()
                ),
                "unique_count": int(
                    dataframe[
                        column
                    ]
                    .nunique(
                        dropna=True
                    )
                ),
            }
            for column
            in dataframe.columns
        ],
        "null_counts": (
            null_counts
        ),
        "missing_cells": (
            missing_cells
        ),
        "duplicate_rows": (
            duplicate_rows
        ),
        "completeness_percentage": (
            round(
                completeness,
                2,
            )
        ),
        "uniqueness_percentage": (
            round(
                uniqueness,
                2,
            )
        ),
        "data_quality_score": (
            quality_score
        ),
        "numeric_summary": (
            numeric_summary(
                dataframe
            )
        ),
        "categorical_summary": (
            categorical_summary(
                dataframe
            )
        ),
        "business_metrics": (
            detect_business_metrics(
                dataframe
            )
        ),
        "sample_rows": (
            dataframe_sample(
                dataframe,
                limit=20,
            )
        ),
    }


# =========================================================
# AUTOMATIC PIPELINE GENERATION
# =========================================================

def build_file_pipeline_steps(
    file_name: str,
    profile: Dict[str, Any],
) -> List[dict]:
    """
    Generate a realistic file-processing workflow.

    These steps describe the actual local processing already
    performed by this page and remain compatible with the
    existing DataDoctor pipeline engine.
    """

    bronze_id = new_id(
        "step"
    )

    silver_id = new_id(
        "step"
    )

    gold_id = new_id(
        "step"
    )

    bronze_code = (
        "# Bronze ingestion\n"
        f"# Source file: {file_name}\n"
        f"# Detected rows: "
        f"{profile['row_count']}\n"
        f"# Detected columns: "
        f"{profile['column_count']}\n"
        "\n"
        "# The real uploaded file was read and profiled by "
        "DataDoctor AI.\n"
        "bronze_df = uploaded_dataframe.copy()\n"
        "bronze_df['ingestion_file'] = "
        f"'{file_name}'"
    )

    silver_code = (
        "# Silver validation and standardization\n"
        "silver_df = bronze_df.drop_duplicates().copy()\n"
        "\n"
        "# Standardized field names, null profiling, "
        "schema detection,\n"
        "# completeness checks, uniqueness checks, and "
        "duplicate analysis\n"
        "# were calculated from the uploaded dataset."
    )

    gold_code = (
        "# Gold business metrics\n"
        "# Verified totals and business KPIs are calculated "
        "only when\n"
        "# suitable source columns exist.\n"
        "\n"
        "gold_metrics = {\n"
        f"    'row_count': "
        f"{profile['row_count']},\n"
        f"    'data_quality_score': "
        f"{profile['data_quality_score']},\n"
        f"    'business_metrics': "
        f"{repr(profile['business_metrics'])},\n"
        "}"
    )

    return [
        Step(
            id=bronze_id,
            name=(
                "Bronze — ingest uploaded file"
            ),
            step_type="source",
            engine="pyspark",
            code=bronze_code,
            depends_on=[],
        ).__dict__,
        Step(
            id=silver_id,
            name=(
                "Silver — validate and standardize"
            ),
            step_type="transform",
            engine="pyspark",
            code=silver_code,
            depends_on=[
                bronze_id
            ],
        ).__dict__,
        Step(
            id=gold_id,
            name=(
                "Gold — calculate business outcomes"
            ),
            step_type="sink",
            engine="pyspark",
            code=gold_code,
            depends_on=[
                silver_id
            ],
        ).__dict__,
    ]


# =========================================================
# REAL ENTERPRISE FILE INGESTION
# =========================================================

st.subheader(
    "📂 Enterprise file ingestion"
)

st.caption(
    "Upload a real CSV, Excel, JSON, or Parquet file. "
    "DataDoctor AI reads the actual records, detects the "
    "schema, profiles quality, calculates verified metrics, "
    "and can generate a Bronze → Silver → Gold pipeline."
)

uploaded_file = st.file_uploader(
    "Upload enterprise data",
    type=[
        "csv",
        "xlsx",
        "xls",
        "json",
        "parquet",
    ],
    help=(
        "The uploaded file is processed in the current "
        "application session."
    ),
)

if uploaded_file is not None:

    current_bytes = (
        uploaded_file.getvalue()
    )

    current_hash = (
        hashlib
        .sha256(
            current_bytes
        )
        .hexdigest()
    )

    should_process = (
        current_hash
        != st.session_state
        .uploaded_file_hash
    )

    if should_process:

        try:

            with st.spinner(
                "Reading and profiling the real file..."
            ):

                dataframe = (
                    read_uploaded_file(
                        uploaded_file
                    )
                )

                if dataframe.empty:

                    st.warning(
                        "The file was read successfully, "
                        "but it contains no data rows."
                    )

                profile = (
                    build_profile(
                        dataframe=(
                            dataframe
                        ),
                        file_name=(
                            uploaded_file.name
                        ),
                        file_hash=(
                            current_hash
                        ),
                    )
                )

                st.session_state[
                    "uploaded_dataframe"
                ] = dataframe

                st.session_state[
                    "uploaded_file_name"
                ] = (
                    uploaded_file.name
                )

                st.session_state[
                    "uploaded_file_hash"
                ] = (
                    current_hash
                )

                st.session_state[
                    "dataset_context"
                ] = profile

                st.session_state[
                    "data_profile"
                ] = profile

                st.session_state[
                    "uploaded_dataset_profile"
                ] = profile

                st.session_state[
                    "business_context"
                ] = (
                    profile[
                        "business_metrics"
                    ]
                )

                st.session_state[
                    "business_outcomes"
                ] = (
                    profile[
                        "business_metrics"
                    ]
                )

            st.success(
                f"Successfully read and profiled "
                f"'{uploaded_file.name}'."
            )

        except Exception as error:

            st.session_state[
                "uploaded_dataframe"
            ] = None

            st.session_state[
                "uploaded_file_name"
            ] = None

            st.session_state[
                "uploaded_file_hash"
            ] = None

            st.session_state[
                "dataset_context"
            ] = None

            st.session_state[
                "data_profile"
            ] = None

            st.session_state[
                "business_context"
            ] = None

            st.session_state[
                "business_outcomes"
            ] = None

            st.error(
                "The uploaded file could not be processed."
            )

            st.exception(
                error
            )


dataframe = (
    st.session_state.get(
        "uploaded_dataframe"
    )
)

profile = (
    st.session_state.get(
        "dataset_context"
    )
)


if (
    isinstance(
        dataframe,
        pd.DataFrame,
    )
    and isinstance(
        profile,
        dict,
    )
):

    metric_1, metric_2, metric_3, metric_4 = (
        st.columns(4)
    )

    metric_1.metric(
        "Rows",
        f"{profile['row_count']:,}",
    )

    metric_2.metric(
        "Columns",
        profile[
            "column_count"
        ],
    )

    metric_3.metric(
        "Duplicate rows",
        f"{profile['duplicate_rows']:,}",
    )

    quality_score = float(
        profile["data_quality_score"]
    )

    quality_score_display = (
        str(int(quality_score))
        if quality_score.is_integer()
        else f"{quality_score:.1f}"
    )

    metric_4.metric(
        "Data quality",
        f"{quality_score_display}/100",
    )

    preview_tab, schema_tab, quality_tab, business_tab = (
        st.tabs(
            [
                "Data preview",
                "Detected schema",
                "Quality profile",
                "Business outcomes",
            ]
        )
    )

    with preview_tab:

        st.dataframe(
            dataframe.head(
                100
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.caption(
            "Showing up to the first 100 records."
        )

    with schema_tab:

        schema_dataframe = (
            pd.DataFrame(
                profile[
                    "columns"
                ]
            )
        )

        st.dataframe(
            schema_dataframe,
            use_container_width=True,
            hide_index=True,
        )

    with quality_tab:

        quality_1, quality_2, quality_3 = (
            st.columns(3)
        )

        quality_1.metric(
            "Completeness",
            (
                f"{profile['completeness_percentage']}"
                "%"
            ),
        )

        quality_2.metric(
            "Uniqueness",
            (
                f"{profile['uniqueness_percentage']}"
                "%"
            ),
        )

        quality_3.metric(
            "Missing cells",
            f"{profile['missing_cells']:,}",
        )

        null_dataframe = (
            pd.DataFrame(
                [
                    {
                        "Column": column,
                        "Missing values": count,
                    }
                    for column, count
                    in profile[
                        "null_counts"
                    ].items()
                ]
            )
        )

        st.markdown(
            "**Missing values by column**"
        )

        st.dataframe(
            null_dataframe,
            use_container_width=True,
            hide_index=True,
        )

    with business_tab:

        business_metrics = (
            profile.get(
                "business_metrics",
                {},
            )
        )

        if not business_metrics:

            st.info(
                "No standard sales, revenue, cost, profit, "
                "or quantity fields were detected. The "
                "dataset is still available for schema, "
                "quality, and RAG analysis."
            )

        else:

            business_columns = (
                st.columns(4)
            )

            if (
                "total_sales"
                in business_metrics
            ):

                business_columns[
                    0
                ].metric(
                    "Total sales",
                    (
                        f"{business_metrics['total_sales']:,.2f}"
                    ),
                )

            if (
                "total_cost"
                in business_metrics
            ):

                business_columns[
                    1
                ].metric(
                    "Total cost",
                    (
                        f"{business_metrics['total_cost']:,.2f}"
                    ),
                )

            if (
                "total_profit"
                in business_metrics
            ):

                business_columns[
                    2
                ].metric(
                    "Total profit",
                    (
                        f"{business_metrics['total_profit']:,.2f}"
                    ),
                )

            if (
                "profit_rate_percentage"
                in business_metrics
            ):

                business_columns[
                    3
                ].metric(
                    "Profit rate",
                    (
                        f"{business_metrics['profit_rate_percentage']:.2f}"
                        "%"
                    ),
                )

            if (
                "highest_profit_region"
                in business_metrics
            ):

                highest_region = (
                    business_metrics[
                        "highest_profit_region"
                    ]
                )

                st.success(
                    "Highest-profit region: "
                    f"{highest_region['region']} "
                    "with verified profit of "
                    f"{highest_region['profit']:,.2f}."
                )

            if (
                "highest_profit_product"
                in business_metrics
            ):

                highest_product = (
                    business_metrics[
                        "highest_profit_product"
                    ]
                )

                st.success(
                    "Highest-profit product: "
                    f"{highest_product['product']} "
                    "with verified profit of "
                    f"{highest_product['profit']:,.2f}."
                )

            with st.expander(
                "View all verified business metrics"
            ):

                st.json(
                    business_metrics
                )

    if st.button(
        "⚙️ Generate Bronze → Silver → Gold steps",
        type="primary",
        use_container_width=True,
    ):

        st.session_state[
            "draft_steps"
        ] = (
            build_file_pipeline_steps(
                file_name=(
                    profile[
                        "file_name"
                    ]
                ),
                profile=(
                    profile
                ),
            )
        )

        st.success(
            "Generated a three-layer enterprise pipeline "
            "from the uploaded file."
        )

        st.rerun()


st.divider()


# =========================================================
# EXISTING QUICK-START TEMPLATES
# =========================================================

with st.expander(
    "⚡ Quick start from a template",
    expanded=(
        len(
            st.session_state
            .draft_steps
        )
        == 0
    ),
):

    templates = (
        get_templates()
    )

    cols = st.columns(
        len(
            templates
        )
    )

    for col, (
        name,
        template,
    ) in zip(
        cols,
        templates.items(),
    ):

        with col:

            st.markdown(
                f"**{name}**"
            )

            st.caption(
                template[
                    "description"
                ]
            )

            if st.button(
                "Use template",
                key=(
                    f"tpl-{name}"
                ),
            ):

                st.session_state[
                    "draft_steps"
                ] = (
                    template[
                        "steps"
                    ]
                )

                st.rerun()


# =========================================================
# EXISTING MANUAL STEP BUILDER
# =========================================================

with st.expander(
    "➕ Add a step",
    expanded=True,
):

    c1, c2, c3 = (
        st.columns(
            [
                2,
                1,
                1,
            ]
        )
    )

    step_name = (
        c1.text_input(
            "Step name",
            placeholder=(
                "e.g. Load raw telemetry"
            ),
        )
    )

    step_type = (
        c2.selectbox(
            "Type",
            [
                "source",
                "transform",
                "sink",
            ],
        )
    )

    engine = (
        c3.selectbox(
            "Engine",
            [
                "pyspark",
                "sql",
            ],
        )
    )

    code = st.text_area(
        "Code",
        placeholder=(
            "df = spark.read.format"
            "('delta').load"
            "('/mnt/bronze/telemetry')"
        ),
        height=100,
    )

    existing_names = [
        step[
            "name"
        ]
        for step
        in st.session_state
        .draft_steps
    ]

    depends_on = (
        st.multiselect(
            "Depends on",
            options=(
                existing_names
            ),
        )
    )

    if st.button(
        "Add step",
        type="primary",
    ):

        if (
            not step_name
            or not code
        ):

            st.error(
                "Step name and code are required."
            )

        else:

            dependency_ids = [
                step[
                    "id"
                ]
                for step
                in st.session_state
                .draft_steps
                if step[
                    "name"
                ]
                in depends_on
            ]

            st.session_state[
                "draft_steps"
            ].append(
                Step(
                    id=new_id(
                        "step"
                    ),
                    name=(
                        step_name
                    ),
                    step_type=(
                        step_type
                    ),
                    engine=(
                        engine
                    ),
                    code=(
                        code
                    ),
                    depends_on=(
                        dependency_ids
                    ),
                ).__dict__
            )

            st.rerun()


# =========================================================
# DRAFT PIPELINE STEPS
# =========================================================

st.subheader(
    "Draft pipeline steps"
)

if not st.session_state.draft_steps:

    st.info(
        "No steps added yet."
    )

else:

    for index, step in enumerate(
        st.session_state
        .draft_steps
    ):

        columns = (
            st.columns(
                [
                    5,
                    1,
                ]
            )
        )

        with columns[
            0
        ]:

            dependencies = (
                ", ".join(
                    dependency[
                        "name"
                    ]
                    for dependency
                    in st.session_state
                    .draft_steps
                    if dependency[
                        "id"
                    ]
                    in step[
                        "depends_on"
                    ]
                )
                or "—"
            )

            st.markdown(
                f"**{index + 1}. "
                f"{step['name']}** "
                f"&nbsp;`{step['step_type']}` "
                f"· `{step['engine']}` "
                f"· depends on: "
                f"{dependencies}"
            )

            st.code(
                step[
                    "code"
                ],
                language=(
                    "python"
                    if step[
                        "engine"
                    ]
                    == "pyspark"
                    else "sql"
                ),
            )

        with columns[
            1
        ]:

            if st.button(
                "Remove",
                key=(
                    f"remove-"
                    f"{step['id']}"
                ),
            ):

                st.session_state[
                    "draft_steps"
                ].pop(
                    index
                )

                st.rerun()


# =========================================================
# SAVE AND RUN
# =========================================================

st.divider()

st.subheader(
    "Save & run"
)

p_name = (
    st.text_input(
        "Pipeline name",
        placeholder=(
            "e.g. "
            "bronze-silver-gold-telemetry"
        ),
    )
)

p_desc = (
    st.text_input(
        "Description",
        placeholder=(
            "Daily incremental load with "
            "SCD2 dimension merge"
        ),
    )
)

c1, c2 = (
    st.columns(2)
)

with c1:

    if st.button(
        "💾 Save pipeline",
        disabled=(
            not st.session_state
            .draft_steps
        ),
    ):

        if not p_name:

            st.error(
                "Give the pipeline a name."
            )

        else:

            pipeline = (
                Pipeline(
                    id=new_id(
                        "pipe"
                    ),
                    name=(
                        p_name
                    ),
                    description=(
                        p_desc
                    ),
                    steps=(
                        st.session_state
                        .draft_steps
                    ),
                )
            )

            pipeline_dict = (
                pipeline.to_dict()
            )

            if (
                isinstance(
                    profile,
                    dict,
                )
                and st.session_state.get(
                    "uploaded_file_name"
                )
            ):

                pipeline_dict[
                    "source_file"
                ] = (
                    st.session_state[
                        "uploaded_file_name"
                    ]
                )

                pipeline_dict[
                    "dataset_profile"
                ] = (
                    profile
                )

            store.save_pipeline(
                pipeline_dict
            )

            st.session_state[
                "draft_steps"
            ] = []

            st.success(
                f"Saved pipeline "
                f"'{p_name}'."
            )

            st.rerun()


with c2:

    saved = (
        store.load_pipelines()
    )

    options = {
        pipeline[
            "name"
        ]: pipeline
        for pipeline
        in saved
    }

    if options:

        chosen = (
            st.selectbox(
                "Run a saved pipeline",
                list(
                    options.keys()
                ),
            )
        )

        if st.button(
            "▶️ Run pipeline",
            type="primary",
        ):

            selected_pipeline = (
                options[
                    chosen
                ]
            )

            with st.spinner(
                f"Running "
                f"'{chosen}'..."
            ):

                run = (
                    run_pipeline(
                        selected_pipeline
                    )
                )

            if (
                run[
                    "status"
                ]
                == "SUCCEEDED"
            ):

                st.success(
                    f"Run "
                    f"{run['id']} "
                    "succeeded."
                )

                if (
                    selected_pipeline.get(
                        "dataset_profile"
                    )
                ):

                    restored_profile = (
                        selected_pipeline[
                            "dataset_profile"
                        ]
                    )

                    st.session_state[
                        "dataset_context"
                    ] = (
                        restored_profile
                    )

                    st.session_state[
                        "data_profile"
                    ] = (
                        restored_profile
                    )

                    st.session_state[
                        "uploaded_dataset_profile"
                    ] = (
                        restored_profile
                    )

                    st.session_state[
                        "business_context"
                    ] = (
                        restored_profile.get(
                            "business_metrics",
                            {},
                        )
                    )

                    st.session_state[
                        "business_outcomes"
                    ] = (
                        restored_profile.get(
                            "business_metrics",
                            {},
                        )
                    )

                    st.info(
                        "Verified dataset metrics are now "
                        "available to Conversational AI "
                        "in this application session."
                    )

            else:

                st.error(
                    f"Run "
                    f"{run['id']} "
                    "finished with status "
                    f"{run['status']}. "
                    "See Monitor & Repair."
                )