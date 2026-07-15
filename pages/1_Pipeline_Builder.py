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
from pathlib import Path
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

if "uploaded_source_local_path" not in st.session_state:
    st.session_state.uploaded_source_local_path = None

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
        "operating_cost",
    ]

    profit_candidates = [
        "profit",
        "total_profit",
        "net_profit",
        "gross_profit",
    ]

    quantity_candidates = [
        "quantity",
        "qty",
        "units",
        "units_sold",
    ]

    region_candidates = [
        "region",
        "sales_region",
        "market",
        "location",
    ]

    product_candidates = [
        "product",
        "product_name",
        "item",
        "item_name",
    ]


    def first_existing(
        candidates: List[str],
    ) -> Optional[str]:
        """
        Return the first candidate column found.
        """

        for candidate in candidates:

            if candidate in columns:

                return candidate

        return None


    sales_column = first_existing(
        sales_candidates
    )

    cost_column = first_existing(
        cost_candidates
    )

    profit_column = first_existing(
        profit_candidates
    )

    quantity_column = first_existing(
        quantity_candidates
    )

    region_column = first_existing(
        region_candidates
    )

    product_column = first_existing(
        product_candidates
    )


    if sales_column:

        sales_series = pd.to_numeric(
            dataframe[
                sales_column
            ],
            errors="coerce",
        )

        metrics[
            "sales_column"
        ] = sales_column

        metrics[
            "total_sales"
        ] = float(
            sales_series.sum()
        )

        metrics[
            "average_sales"
        ] = float(
            sales_series.mean()
        )


    if cost_column:

        cost_series = pd.to_numeric(
            dataframe[
                cost_column
            ],
            errors="coerce",
        )

        metrics[
            "cost_column"
        ] = cost_column

        metrics[
            "total_cost"
        ] = float(
            cost_series.sum()
        )


    if profit_column:

        profit_series = pd.to_numeric(
            dataframe[
                profit_column
            ],
            errors="coerce",
        )

        metrics[
            "profit_column"
        ] = profit_column

        metrics[
            "total_profit"
        ] = float(
            profit_series.sum()
        )

        metrics[
            "average_profit"
        ] = float(
            profit_series.mean()
        )


    elif (
        sales_column
        and cost_column
    ):

        calculated_profit = (
            pd.to_numeric(
                dataframe[
                    sales_column
                ],
                errors="coerce",
            )
            -
            pd.to_numeric(
                dataframe[
                    cost_column
                ],
                errors="coerce",
            )
        )

        metrics[
            "profit_column"
        ] = (
            "calculated_from_sales_minus_cost"
        )

        metrics[
            "total_profit"
        ] = float(
            calculated_profit.sum()
        )

        metrics[
            "average_profit"
        ] = float(
            calculated_profit.mean()
        )


    if quantity_column:

        quantity_series = pd.to_numeric(
            dataframe[
                quantity_column
            ],
            errors="coerce",
        )

        metrics[
            "quantity_column"
        ] = quantity_column

        metrics[
            "total_quantity"
        ] = float(
            quantity_series.sum()
        )

        metrics[
            "average_quantity"
        ] = float(
            quantity_series.mean()
        )


    if (
        "total_sales"
        in metrics
        and metrics[
            "total_sales"
        ] != 0
        and "total_profit"
        in metrics
    ):

        metrics[
            "profit_rate_percentage"
        ] = float(
            (
                metrics[
                    "total_profit"
                ]
                /
                metrics[
                    "total_sales"
                ]
            )
            * 100
        )


    if (
        region_column
        and profit_column
    ):

        region_profit = (
            dataframe
            .assign(
                __verified_profit=(
                    pd.to_numeric(
                        dataframe[
                            profit_column
                        ],
                        errors="coerce",
                    )
                )
            )
            .groupby(
                region_column,
                dropna=False,
            )[
                "__verified_profit"
            ]
            .sum()
            .sort_values(
                ascending=False
            )
        )

        if not region_profit.empty:

            highest_region = (
                region_profit.index[
                    0
                ]
            )

            metrics[
                "highest_profit_region"
            ] = {
                "region": str(
                    highest_region
                ),
                "profit": float(
                    region_profit.iloc[
                        0
                    ]
                ),
            }

            metrics[
                "profit_by_region"
            ] = {
                str(
                    key
                ): float(
                    value
                )
                for key, value
                in region_profit.items()
            }


    if (
        product_column
        and profit_column
    ):

        product_profit = (
            dataframe
            .assign(
                __verified_profit=(
                    pd.to_numeric(
                        dataframe[
                            profit_column
                        ],
                        errors="coerce",
                    )
                )
            )
            .groupby(
                product_column,
                dropna=False,
            )[
                "__verified_profit"
            ]
            .sum()
            .sort_values(
                ascending=False
            )
        )

        if not product_profit.empty:

            highest_product = (
                product_profit.index[
                    0
                ]
            )

            metrics[
                "highest_profit_product"
            ] = {
                "product": str(
                    highest_product
                ),
                "profit": float(
                    product_profit.iloc[
                        0
                    ]
                ),
            }

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


    return metrics
def build_profile(
    dataframe: pd.DataFrame,
    file_name: str,
    file_hash: str,
) -> Dict[str, Any]:
    """
    Build a verified dataset profile for:

    - Pipeline generation
    - Data-quality reporting
    - Business analysis
    - Conversational AI grounding
    - RAG context
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

    total_cells = (
        row_count
        * column_count
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
        for column
        in dataframe.columns
    }

    missing_cells = int(
        sum(
            null_counts.values()
        )
    )

    duplicate_rows = int(
        dataframe
        .duplicated()
        .sum()
    )

    completeness_percentage = (
        round(
            (
                1
                -
                (
                    missing_cells
                    /
                    total_cells
                )
            )
            * 100,
            2,
        )
        if total_cells
        else 100.0
    )

    uniqueness_percentage = (
        round(
            (
                1
                -
                (
                    duplicate_rows
                    /
                    row_count
                )
            )
            * 100,
            2,
        )
        if row_count
        else 100.0
    )

    data_quality_score = round(
        (
            completeness_percentage
            * 0.7
        )
        +
        (
            uniqueness_percentage
            * 0.3
        ),
        2,
    )

    columns = []

    for column in dataframe.columns:

        series = dataframe[
            column
        ]

        columns.append(
            {
                "name": str(
                    column
                ),
                "dtype": str(
                    series.dtype
                ),
                "nullable": bool(
                    series
                    .isna()
                    .any()
                ),
                "missing_values": int(
                    series
                    .isna()
                    .sum()
                ),
                "unique_values": int(
                    series
                    .nunique(
                        dropna=True
                    )
                ),
            }
        )

    profile = {
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
        "columns": (
            columns
        ),
        "column_names": (
            list(
                dataframe.columns
            )
        ),
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
            completeness_percentage
        ),
        "uniqueness_percentage": (
            uniqueness_percentage
        ),
        "data_quality_score": (
            data_quality_score
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
        "sample_rows": (
            dataframe_sample(
                dataframe,
                limit=20,
            )
        ),
        "business_metrics": (
            detect_business_metrics(
                dataframe
            )
        ),
    }

    return profile


# =========================================================
# FILE-BASED PIPELINE GENERATION
# =========================================================

def build_file_pipeline_steps(
    file_name: str,
    profile: Dict[str, Any],
) -> List[dict]:
    """
    Generate a dependency-aware Bronze → Silver → Gold
    pipeline from the uploaded dataset profile.
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
        "# Bronze layer: raw file ingestion\n"
        f"source_file = {file_name!r}\n"
        "bronze_df = uploaded_dataframe.copy()\n"
        "\n"
        "# Preserve source records and ingestion metadata.\n"
        "bronze_row_count = len(bronze_df)"
    )

    silver_code = (
        "# Silver layer: cleanse and validate\n"
        "silver_df = bronze_df.copy()\n"
        "\n"
        "# Normalize field names.\n"
        "silver_df.columns = [\n"
        "    str(column).strip().lower().replace(' ', '_')\n"
        "    for column in silver_df.columns\n"
        "]\n"
        "\n"
        "# Remove exact duplicate records.\n"
        "silver_df = silver_df.drop_duplicates()\n"
        "\n"
        "# Preserve valid rows for downstream publishing.\n"
        "silver_row_count = len(silver_df)"
    )

    business_metrics = (
        profile.get(
            "business_metrics",
            {},
        )
    )

    if business_metrics:

        gold_code = (
            "# Gold layer: publish verified business metrics\n"
            "gold_metrics = "
            f"{business_metrics!r}\n"
            "\n"
            "# Metrics were calculated from the complete\n"
            "# uploaded dataset during profiling."
        )

    else:

        gold_code = (
            "# Gold layer: publish curated dataset metrics\n"
            "gold_metrics = {\n"
            f"    'row_count': {profile.get('row_count', 0)},\n"
            f"    'column_count': {profile.get('column_count', 0)},\n"
            "    'data_quality_score': "
            f"{profile.get('data_quality_score', 0)},\n"
            "}\n"
        )

    return [
        Step(
            id=bronze_id,
            name=(
                "Bronze: ingest uploaded file"
            ),
            step_type="source",
            engine="pyspark",
            code=bronze_code,
            depends_on=[],
        ).__dict__,
        Step(
            id=silver_id,
            name=(
                "Silver: cleanse and validate"
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
                "Gold: publish business outcomes"
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
# UPLOADED FILE INGESTION
# =========================================================

st.subheader(
    "📂 Build from enterprise data"
)

st.caption(
    "Upload a real dataset to profile its schema, assess "
    "quality, calculate verified business outcomes, and "
    "generate Bronze → Silver → Gold pipeline steps."
)

uploaded_file = st.file_uploader(
    "Upload CSV, Excel, JSON, or Parquet",
    type=[
        "csv",
        "xlsx",
        "xls",
        "json",
        "parquet",
    ],
)


if uploaded_file is not None:

    current_hash = (
        hashlib.sha256(
            uploaded_file.getvalue()
        )
        .hexdigest()
    )

    upload_dir = Path(__file__).resolve().parent.parent / "data" / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in uploaded_file.name)
    local_source_path = upload_dir / f"{current_hash[:12]}_{safe_name}"
    if not local_source_path.exists():
        local_source_path.write_bytes(uploaded_file.getvalue())
    st.session_state["uploaded_source_local_path"] = str(local_source_path)

    should_process = (
        current_hash
        != st.session_state.get(
            "uploaded_file_hash"
        )
    )

    if should_process:

        try:

            with st.spinner(
                "Reading and profiling the uploaded dataset..."
            ):

                dataframe = (
                    read_uploaded_file(
                        uploaded_file
                    )
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

                # Keep only a lightweight preview in
                # Streamlit session memory.
                #
                # The complete uploaded dataset has already
                # been used above to calculate:
                # - row count
                # - schema
                # - quality metrics
                # - numeric summaries
                # - business outcomes
                st.session_state[
                    "uploaded_dataframe"
                ] = dataframe.head(
                    100
                ).copy()

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
                ] = (
                    profile
                )

                st.session_state[
                    "data_profile"
                ] = (
                    profile
                )

                st.session_state[
                    "uploaded_dataset_profile"
                ] = (
                    profile
                )

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

                try:

                    store.save_uploaded_file(
                        {
                            "id": new_id(
                                "upload"
                            ),
                            "file_name": (
                                uploaded_file.name
                            ),
                            "file_hash": (
                                current_hash
                            ),
                            "row_count": (
                                profile[
                                    "row_count"
                                ]
                            ),
                            "column_count": (
                                profile[
                                    "column_count"
                                ]
                            ),
                            "profile": (
                                profile
                            ),
                        }
                    )

                except Exception:

                    pass

                try:

                    store.save_business_insight(
                        {
                            "id": new_id(
                                "insight"
                            ),
                            "file_name": (
                                uploaded_file.name
                            ),
                            "business_metrics": (
                                profile[
                                    "business_metrics"
                                ]
                            ),
                            "data_quality_score": (
                                profile[
                                    "data_quality_score"
                                ]
                            ),
                        }
                    )

                except Exception:

                    pass

                # Remove the temporary full DataFrame after
                # profiling. Only the 100-row preview remains
                # in Streamlit session state.
                del dataframe

        except Exception as error:

            st.error(
                "Unable to process the uploaded file: "
                f"{error}"
            )

            st.stop()
            # =========================================================
# DISPLAY UPLOADED DATASET PROFILE
# =========================================================

dataframe = st.session_state.get(
    "uploaded_dataframe"
)

profile = st.session_state.get(
    "uploaded_dataset_profile"
)


if (
    dataframe is not None
    and profile
):

    st.success(
        "Dataset loaded and profiled successfully."
    )


    metric_1, metric_2, metric_3, metric_4 = (
        st.columns(
            4
        )
    )


    metric_1.metric(
        "Rows",
        f"{profile['row_count']:,}",
    )


    metric_2.metric(
        "Columns",
        f"{profile['column_count']:,}",
    )


    metric_3.metric(
        "Duplicates",
        f"{profile['duplicate_rows']:,}",
    )


    quality_score = float(
        profile[
            "data_quality_score"
        ]
    )


    quality_score_display = (
        str(
            int(
                quality_score
            )
        )
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


    # =====================================================
    # DATA PREVIEW TAB
    # =====================================================

    with preview_tab:

        st.dataframe(
            dataframe.head(
                100
            ),
            use_container_width=True,
            hide_index=True,
        )

        st.caption(
            "Showing up to the first 100 records. "
            "The complete uploaded dataset was used for "
            "profiling and verified metric calculations."
        )


    # =====================================================
    # DETECTED SCHEMA TAB
    # =====================================================

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


    # =====================================================
    # QUALITY PROFILE TAB
    # =====================================================

    with quality_tab:

        quality_1, quality_2, quality_3 = (
            st.columns(
                3
            )
        )


        quality_1.metric(
            "Completeness",
            (
                f"{profile['completeness_percentage']}%"
            ),
        )


        quality_2.metric(
            "Uniqueness",
            (
                f"{profile['uniqueness_percentage']}%"
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


    # =====================================================
    # BUSINESS OUTCOMES TAB
    # =====================================================

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
                "quality, pipeline generation, and RAG "
                "analysis."
            )


        else:

            business_columns = (
                st.columns(
                    4
                )
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
                        f"{business_metrics['profit_rate_percentage']:.2f}%"
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


    # =====================================================
    # GENERATE BRONZE → SILVER → GOLD
    # =====================================================

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


    template_name = (
        st.selectbox(
            "Template",
            options=list(
                templates.keys()
            ),
        )
    )


    selected_template = (
        templates[
            template_name
        ]
    )


    st.caption(
        selected_template[
            "description"
        ]
    )


    if st.button(
        "Use this template",
        use_container_width=True,
    ):

        st.session_state[
            "draft_steps"
        ] = [

            dict(
                step
            )

            for step

            in selected_template[
                "steps"
            ]

        ]


        st.success(
            "Template loaded into the pipeline draft."
        )


        st.rerun()


st.divider()


# =========================================================
# MANUAL STEP BUILDER
# =========================================================

st.subheader(
    "Add a pipeline step"
)


with st.form(
    "add_step_form",
    clear_on_submit=True,
):

    step_name = (
        st.text_input(
            "Step name",
            placeholder=(
                "Example: Validate customer records"
            ),
        )
    )


    form_column_1, form_column_2 = (
        st.columns(
            2
        )
    )


    step_type = (
        form_column_1.selectbox(
            "Step type",
            options=[
                "source",
                "transform",
                "sink",
            ],
        )
    )


    engine = (
        form_column_2.selectbox(
            "Engine",
            options=[
                "pyspark",
                "sql",
            ],
        )
    )


    code = (
        st.text_area(
            "Code",
            height=180,
            placeholder=(
                "Enter PySpark or SQL logic..."
            ),
        )
    )


    existing_steps = (
        st.session_state[
            "draft_steps"
        ]
    )


    dependency_options = {

        (
            f"{step.get('name', 'Unnamed step')} "
            f"· {step.get('id', '')}"
        ):

        step.get(
            "id"
        )

        for step

        in existing_steps

        if step.get(
            "id"
        )

    }


    selected_dependencies = (
        st.multiselect(
            "Depends on",
            options=list(
                dependency_options.keys()
            ),
            help=(
                "Select upstream steps that must succeed "
                "before this step can execute."
            ),
        )
    )


    add_step = (
        st.form_submit_button(
            "➕ Add step",
            use_container_width=True,
        )
    )


if add_step:

    if not step_name.strip():

        st.error(
            "Enter a step name."
        )


    elif not code.strip():

        st.error(
            "Enter step code."
        )


    else:

        new_step = (
            Step(
                id=new_id(
                    "step"
                ),
                name=(
                    step_name.strip()
                ),
                step_type=(
                    step_type
                ),
                engine=(
                    engine
                ),
                code=(
                    code.strip()
                ),
                depends_on=[

                    dependency_options[
                        label
                    ]

                    for label

                    in selected_dependencies

                ],
            )
        )


        st.session_state[
            "draft_steps"
        ].append(
            new_step.__dict__
        )


        st.success(
            "Pipeline step added."
        )


        st.rerun()
        # =========================================================
# PIPELINE DRAFT
# =========================================================

st.divider()

st.subheader(
    "Pipeline draft"
)


draft_steps = (
    st.session_state[
        "draft_steps"
    ]
)


if not draft_steps:

    st.info(
        "No pipeline steps have been added yet. "
        "Upload a dataset, use a quick-start template, "
        "or add steps manually."
    )


else:

    step_name_by_id = {

        step.get(
            "id"
        ):

        step.get(
            "name",
            "Unnamed step",
        )

        for step

        in draft_steps

    }


    for index, step in enumerate(
        draft_steps,
        start=1,
    ):

        step_id = (
            step.get(
                "id",
                "",
            )
        )

        step_name = (
            step.get(
                "name",
                "Unnamed step",
            )
        )

        step_type_value = (
            step.get(
                "step_type",
                "transform",
            )
        )

        engine_value = (
            step.get(
                "engine",
                "pyspark",
            )
        )

        dependency_ids = (
            step.get(
                "depends_on",
                [],
            )
        )

        dependency_names = [

            step_name_by_id.get(
                dependency_id,
                dependency_id,
            )

            for dependency_id

            in dependency_ids

        ]


        with st.expander(
            (
                f"{index}. "
                f"{step_name} "
                f"· {step_type_value.upper()} "
                f"· {engine_value.upper()}"
            ),
            expanded=True,
        ):

            detail_1, detail_2, detail_3 = (
                st.columns(
                    3
                )
            )


            detail_1.markdown(
                "**Step type**"
            )

            detail_1.write(
                step_type_value
            )


            detail_2.markdown(
                "**Engine**"
            )

            detail_2.write(
                engine_value
            )


            detail_3.markdown(
                "**Dependencies**"
            )

            detail_3.write(
                (
                    ", ".join(
                        dependency_names
                    )
                    if dependency_names
                    else "None"
                )
            )


            st.markdown(
                "**Code**"
            )


            st.code(
                step.get(
                    "code",
                    "",
                ),
                language=(
                    "sql"
                    if engine_value
                    == "sql"
                    else "python"
                ),
            )


            if st.button(
                "🗑️ Remove step",
                key=(
                    f"remove_step_"
                    f"{step_id}_"
                    f"{index}"
                ),
                use_container_width=False,
            ):

                removed_step_id = (
                    step_id
                )


                updated_steps = []


                for current_step in draft_steps:

                    if (
                        current_step.get(
                            "id"
                        )
                        == removed_step_id
                    ):

                        continue


                    updated_step = dict(
                        current_step
                    )


                    updated_step[
                        "depends_on"
                    ] = [

                        dependency_id

                        for dependency_id

                        in updated_step.get(
                            "depends_on",
                            [],
                        )

                        if dependency_id
                        != removed_step_id

                    ]


                    updated_steps.append(
                        updated_step
                    )


                st.session_state[
                    "draft_steps"
                ] = (
                    updated_steps
                )


                st.success(
                    "Pipeline step removed."
                )


                st.rerun()


    if st.button(
        "🧹 Clear pipeline draft",
        use_container_width=True,
    ):

        st.session_state[
            "draft_steps"
        ] = []


        st.success(
            "Pipeline draft cleared."
        )


        st.rerun()


# =========================================================
# SAVE AND EXECUTE PIPELINE
# =========================================================

st.divider()

st.subheader(
    "Save and execute"
)


pipeline_name = (
    st.text_input(
        "Pipeline name",
        placeholder=(
            "Example: Sales Bronze Silver Gold Pipeline"
        ),
    )
)


pipeline_description = (
    st.text_area(
        "Pipeline description",
        placeholder=(
            "Describe the source, transformations, "
            "business purpose, and expected output."
        ),
        height=110,
    )
)


save_column, run_column = (
    st.columns(
        2
    )
)


save_pipeline_button = (
    save_column.button(
        "💾 Save pipeline",
        use_container_width=True,
        disabled=(
            len(
                draft_steps
            )
            == 0
        ),
    )
)


save_and_run_button = (
    run_column.button(
        "▶️ Save and run",
        type="primary",
        use_container_width=True,
        disabled=(
            len(
                draft_steps
            )
            == 0
        ),
    )
)


def create_pipeline_record() -> dict:
    """
    Create a JSON-safe pipeline record from the current
    Pipeline Builder draft.

    The uploaded dataset profile is included when the draft
    was generated from a real file.
    """

    clean_name = (
        pipeline_name.strip()
    )


    if not clean_name:

        raise ValueError(
            "Enter a pipeline name before saving."
        )


    pipeline = (
        Pipeline(
            id=new_id(
                "pipeline"
            ),
            name=(
                clean_name
            ),
            description=(
                pipeline_description
                .strip()
            ),
            steps=[

                Step(
                    id=(
                        step[
                            "id"
                        ]
                    ),
                    name=(
                        step[
                            "name"
                        ]
                    ),
                    step_type=(
                        step[
                            "step_type"
                        ]
                    ),
                    engine=(
                        step.get(
                            "engine",
                            "pyspark",
                        )
                    ),
                    code=(
                        step.get(
                            "code",
                            "",
                        )
                    ),
                    depends_on=list(
                        step.get(
                            "depends_on",
                            [],
                        )
                    ),
                )

                for step

                in st.session_state[
                    "draft_steps"
                ]

            ],
        )
    )


    pipeline_record = (
        pipeline.to_dict()
    )


    current_profile = (
        st.session_state.get(
            "uploaded_dataset_profile"
        )
    )


    if current_profile:

        pipeline_record[
            "pipeline_mode"
        ] = (
            "FILE_DRIVEN"
        )


        pipeline_record[
            "source_file_name"
        ] = (
            current_profile.get(
                "file_name"
            )
        )


        pipeline_record[
            "source_file_hash"
        ] = (
            current_profile.get(
                "file_hash"
            )
        )


        pipeline_record["source_local_path"] = st.session_state.get(
            "uploaded_source_local_path"
        )


        pipeline_record[
            "dataset_profile"
        ] = (
            current_profile
        )


        pipeline_record[
            "business_metrics"
        ] = (
            current_profile.get(
                "business_metrics",
                {},
            )
        )


    return pipeline_record


# =========================================================
# SAVE PIPELINE
# =========================================================

if save_pipeline_button:

    try:

        pipeline_record = (
            create_pipeline_record()
        )


        store.save_pipeline(
            pipeline_record
        )


        st.success(
            (
                "Pipeline saved successfully: "
                f"{pipeline_record['name']}."
            )
        )


    except Exception as error:

        st.error(
            "Unable to save the pipeline: "
            f"{error}"
        )


# =========================================================
# SAVE AND RUN PIPELINE
# =========================================================

if save_and_run_button:

    try:

        pipeline_record = (
            create_pipeline_record()
        )


        store.save_pipeline(
            pipeline_record
        )


        with st.spinner(
            "Executing the pipeline..."
        ):

            run = (
                run_pipeline(
                    pipeline_record
                )
            )


        final_status = (
            run.get(
                "status",
                "UNKNOWN",
            )
        )


        if final_status in (
            "SUCCEEDED",
            "REPAIRED",
        ):

            st.success(
                (
                    "Pipeline execution completed with "
                    f"status: {final_status}."
                )
            )


        elif final_status == (
            "PARTIALLY_REPAIRED"
        ):

            st.warning(
                "Pipeline execution completed with status: "
                "PARTIALLY_REPAIRED. Review unresolved or "
                "skipped steps in Monitor & Repair."
            )


        elif final_status == "RUNNING":

            st.info(
                "Pipeline execution is still running."
            )


        else:

            st.error(
                (
                    "Pipeline execution completed with "
                    f"status: {final_status}. "
                    "Open Monitor & Repair for step-level "
                    "details."
                )
            )


        st.session_state[
            "last_pipeline_run"
        ] = (
            run
        )


        st.session_state[
            "last_pipeline_id"
        ] = (
            pipeline_record[
                "id"
            ]
        )


        st.session_state[
            "last_run_id"
        ] = (
            run.get(
                "id"
            )
        )


    except Exception as error:

        st.error(
            "Unable to save or execute the pipeline: "
            f"{error}"
        )


# =========================================================
# LAST EXECUTION SUMMARY
# =========================================================

last_run = (
    st.session_state.get(
        "last_pipeline_run"
    )
)


if last_run:

    st.divider()

    st.subheader(
        "Latest execution"
    )


    execution_1, execution_2, execution_3 = (
        st.columns(
            3
        )
    )


    execution_1.metric(
        "Run ID",
        last_run.get(
            "id",
            "—",
        ),
    )


    execution_2.metric(
        "Status",
        last_run.get(
            "status",
            "UNKNOWN",
        ),
    )


    execution_3.metric(
        "Pipeline",
        last_run.get(
            "pipeline_name",
            pipeline_name
            or "—",
        ),
    )


    step_runs = (
        last_run.get(
            "step_runs",
            [],
        )
    )


    if step_runs:

        execution_rows = []


        for step_run in step_runs:

            execution_rows.append(
                {
                    "Step ID": (
                        step_run.get(
                            "step_id",
                            "—",
                        )
                    ),
                    "Status": (
                        step_run.get(
                            "status",
                            "UNKNOWN",
                        )
                    ),
                    "Rows processed": (
                        step_run.get(
                            "rows_processed"
                        )
                    ),
                    "Retry count": (
                        step_run.get(
                            "retry_count",
                            0,
                        )
                    ),
                    "Error": (
                        step_run.get(
                            "error_message"
                        )
                    ),
                }
            )


        st.dataframe(
            pd.DataFrame(
                execution_rows
            ),
            use_container_width=True,
            hide_index=True,
        )


# =========================================================
# MEMORY INFORMATION
# =========================================================

st.caption(
    "Memory-optimized mode: the complete uploaded dataset "
    "is used temporarily for profiling, while only the first "
    "100 rows and the verified dataset profile are retained "
    "in Streamlit session memory."
)