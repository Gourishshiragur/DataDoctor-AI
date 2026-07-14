"""
DataDoctor AI — Enterprise Pipeline Execution Engine

Supports three pipeline execution modes:

1. FILE-DRIVEN PIPELINE
   - Uses the real uploaded dataset profile saved with the pipeline
   - Uses verified source row counts
   - Uses verified duplicate and missing-value results
   - Uses verified data-quality metrics
   - Produces deterministic Bronze → Silver → Gold execution results
   - Does not invent random row counts or failures

2. ENTERPRISE TEMPLATE PIPELINE
   - Runs locally using deterministic orchestration simulation
   - Demonstrates dependency handling, structured logs, monitoring,
     retries, and repair workflows without requiring a Spark cluster

3. CUSTOM PIPELINE
   - Validates custom PySpark/SQL step dependencies
   - Runs through the local orchestration engine
   - Clearly records that execution used the local simulation backend

A free Streamlit application should not silently claim to execute a real
Databricks or Spark cluster. The execution mode is therefore recorded in
run metadata and logs.

To connect a real execution backend later, replace the template/custom
step executor with Databricks Jobs API, Spark Connect, or another
supported execution provider. Dependency ordering, structured logging,
run persistence, monitoring, and repair integration can remain unchanged.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from core import store
from core.models import Run, StepRun, new_id


# =========================================================
# EXECUTION CONSTANTS
# =========================================================

FILE_DRIVEN_MODE = "file-driven"
TEMPLATE_MODE = "enterprise-template"
CUSTOM_MODE = "custom"

REAL_PROFILE_EXECUTION = "verified-local-data"
LOCAL_SIMULATION_EXECUTION = "local-orchestration-simulation"


# =========================================================
# TIME AND LOGGING
# =========================================================

def _now() -> str:
    """
    Return a timezone-aware UTC timestamp.
    """

    return (
        datetime
        .now(
            timezone.utc
        )
        .isoformat()
    )


def _log(
    run_id: str,
    step_id: str,
    level: str,
    message: str,
    **metadata: Any,
) -> None:
    """
    Write a structured pipeline log entry.

    Existing Logs pages remain compatible because the original
    run_id, step_id, level, message, and timestamp fields are
    preserved.
    """

    entry: Dict[str, Any] = {
        "run_id": run_id,
        "step_id": step_id,
        "level": level,
        "message": message,
        "timestamp": _now(),
    }

    for key, value in metadata.items():

        if value is not None:

            entry[
                key
            ] = value

    store.append_log(
        entry
    )


# =========================================================
# SAFE VALUE HELPERS
# =========================================================

def _safe_int(
    value: Any,
    default: int = 0,
) -> int:
    """
    Convert a value to an integer without crashing execution.
    """

    try:

        return int(
            value
        )

    except (
        TypeError,
        ValueError,
    ):

        return default


def _safe_float(
    value: Any,
    default: float = 0.0,
) -> float:
    """
    Convert a value to a float without crashing execution.
    """

    try:

        return float(
            value
        )

    except (
        TypeError,
        ValueError,
    ):

        return default


def _format_score(
    value: Any,
) -> str:
    """
    Format quality scores cleanly.

    Examples:
        100.0 -> 100
        97.8  -> 97.8
        93.25 -> 93.2
    """

    score = _safe_float(
        value
    )

    if score.is_integer():

        return str(
            int(
                score
            )
        )

    return (
        f"{score:.1f}"
    )


# =========================================================
# PIPELINE MODE DETECTION
# =========================================================

def _has_dataset_profile(
    pipeline: Dict[str, Any],
) -> bool:
    """
    Check whether a pipeline contains a real uploaded-file
    profile.
    """

    profile = pipeline.get(
        "dataset_profile"
    )

    return (
        isinstance(
            profile,
            dict,
        )
        and "row_count"
        in profile
        and "column_count"
        in profile
    )


def _detect_pipeline_mode(
    pipeline: Dict[str, Any],
) -> str:
    """
    Determine how the pipeline was created.

    Explicit metadata is preferred. Existing pipelines that
    do not yet contain creation_mode remain supported.
    """

    explicit_mode = str(
        pipeline.get(
            "creation_mode",
            "",
        )
    ).strip().lower()


    if explicit_mode in {
        FILE_DRIVEN_MODE,
        "file",
        "file_driven",
        "file-driven-pipeline",
    }:

        return FILE_DRIVEN_MODE


    if explicit_mode in {
        TEMPLATE_MODE,
        "template",
        "enterprise_template",
        "enterprise-template-pipeline",
    }:

        return TEMPLATE_MODE


    if explicit_mode in {
        CUSTOM_MODE,
        "manual",
        "custom-pipeline",
    }:

        return CUSTOM_MODE


    if _has_dataset_profile(
        pipeline
    ):

        return FILE_DRIVEN_MODE


    pipeline_name = str(
        pipeline.get(
            "name",
            "",
        )
    ).lower()


    description = str(
        pipeline.get(
            "description",
            "",
        )
    ).lower()


    combined_text = (
        f"{pipeline_name} "
        f"{description}"
    )


    template_markers = (
        "medallion",
        "micro-batch",
        "micro batch",
        "scd",
        "slowly changing",
        "aggregation",
        "bronze",
        "silver",
        "gold",
    )


    if any(
        marker
        in combined_text
        for marker
        in template_markers
    ):

        return TEMPLATE_MODE


    return CUSTOM_MODE


def _execution_backend(
    pipeline_mode: str,
) -> str:
    """
    Return the backend label used by monitoring and logs.
    """

    if (
        pipeline_mode
        == FILE_DRIVEN_MODE
    ):

        return (
            REAL_PROFILE_EXECUTION
        )

    return (
        LOCAL_SIMULATION_EXECUTION
    )


# =========================================================
# DEPENDENCY VALIDATION
# =========================================================

def _topological_order(
    steps: List[dict],
) -> List[dict]:
    """
    Return steps in dependency order.

    Also validates:
    - duplicate step IDs
    - missing dependency IDs
    - circular dependencies
    """

    if not isinstance(
        steps,
        list,
    ):

        raise ValueError(
            "Pipeline steps must be provided as a list."
        )


    by_id: Dict[
        str,
        dict,
    ] = {}


    for step in steps:

        step_id = step.get(
            "id"
        )


        if not step_id:

            raise ValueError(
                "Every pipeline step requires an ID."
            )


        if step_id in by_id:

            raise ValueError(
                "Duplicate pipeline step ID detected: "
                f"{step_id}"
            )


        by_id[
            step_id
        ] = step


    visited = set()

    visiting = set()

    ordered_ids: List[
        str
    ] = []


    def visit(
        step_id: str,
    ) -> None:

        if step_id in visited:

            return


        if step_id in visiting:

            raise ValueError(
                "Circular dependency detected at step "
                f"'{step_id}'."
            )


        if step_id not in by_id:

            raise ValueError(
                "Pipeline dependency references missing "
                f"step '{step_id}'."
            )


        visiting.add(
            step_id
        )


        step = by_id[
            step_id
        ]


        for dependency_id in step.get(
            "depends_on",
            [],
        ):

            if dependency_id not in by_id:

                raise ValueError(
                    f"Step '{step.get('name', step_id)}' "
                    "references missing dependency "
                    f"'{dependency_id}'."
                )


            visit(
                dependency_id
            )


        visiting.remove(
            step_id
        )


        visited.add(
            step_id
        )


        ordered_ids.append(
            step_id
        )


    for current_step in steps:

        visit(
            current_step[
                "id"
            ]
        )


    return [
        by_id[
            step_id
        ]
        for step_id
        in ordered_ids
    ]


# =========================================================
# FILE-DRIVEN EXECUTION
# =========================================================

def _file_stage(
    step: Dict[str, Any],
    position: int,
) -> str:
    """
    Determine the file-processing stage represented by a step.
    """

    searchable_text = (
        f"{step.get('name', '')} "
        f"{step.get('step_type', '')} "
        f"{step.get('code', '')}"
    ).lower()


    if (
        "bronze"
        in searchable_text
        or step.get(
            "step_type"
        )
        == "source"
        or position
        == 0
    ):

        return "bronze"


    if (
        "silver"
        in searchable_text
        or step.get(
            "step_type"
        )
        == "transform"
        or position
        == 1
    ):

        return "silver"


    if (
        "gold"
        in searchable_text
        or step.get(
            "step_type"
        )
        == "sink"
    ):

        return "gold"


    return "processing"


def _execute_file_step(
    step: Dict[str, Any],
    run_id: str,
    profile: Dict[str, Any],
    position: int,
) -> Tuple[
    bool,
    Optional[str],
    int,
    Dict[str, Any],
]:
    """
    Execute a deterministic file-driven pipeline stage using
    verified values from the uploaded dataset profile.

    The real file was read and profiled in Pipeline Builder.
    This executor orchestrates those verified results without
    inventing random row counts.
    """

    step_id = step[
        "id"
    ]


    step_name = step.get(
        "name",
        step_id,
    )


    stage = _file_stage(
        step=step,
        position=position,
    )


    source_rows = _safe_int(
        profile.get(
            "row_count"
        )
    )


    source_columns = _safe_int(
        profile.get(
            "column_count"
        )
    )


    duplicate_rows = _safe_int(
        profile.get(
            "duplicate_rows"
        )
    )


    missing_cells = _safe_int(
        profile.get(
            "missing_cells"
        )
    )


    quality_score = _safe_float(
        profile.get(
            "data_quality_score"
        )
    )


    source_file = str(
        profile.get(
            "file_name",
            "uploaded dataset",
        )
    )


    business_metrics = profile.get(
        "business_metrics",
        {},
    )


    if not isinstance(
        business_metrics,
        dict,
    ):

        business_metrics = {}


    _log(
        run_id,
        step_id,
        "INFO",
        (
            f"Starting verified {stage.title()} stage "
            f"'{step_name}'."
        ),
        pipeline_mode=(
            FILE_DRIVEN_MODE
        ),
        execution_backend=(
            REAL_PROFILE_EXECUTION
        ),
        stage=stage,
        source_file=(
            source_file
        ),
    )


    time.sleep(
        0.15
    )


    if stage == "bronze":

        rows_processed = (
            source_rows
        )


        output = {
            "stage": "bronze",
            "source_file": (
                source_file
            ),
            "rows_read": (
                source_rows
            ),
            "rows_processed": (
                source_rows
            ),
            "columns_detected": (
                source_columns
            ),
            "schema_detected": True,
            "status_message": (
                "Uploaded source records were ingested "
                "and registered using the verified file "
                "profile."
            ),
        }


        _log(
            run_id,
            step_id,
            "INFO",
            (
                f"Bronze ingestion completed: "
                f"{source_rows:,} rows and "
                f"{source_columns:,} columns read from "
                f"'{source_file}'."
            ),
            stage="bronze",
            rows_read=(
                source_rows
            ),
            rows_processed=(
                source_rows
            ),
            columns_detected=(
                source_columns
            ),
            source_file=(
                source_file
            ),
        )


        return (
            True,
            None,
            rows_processed,
            output,
        )


    if stage == "silver":

        rows_processed = max(
            source_rows
            - duplicate_rows,
            0,
        )


        output = {
            "stage": "silver",
            "rows_received": (
                source_rows
            ),
            "rows_processed": (
                rows_processed
            ),
            "duplicate_rows_detected": (
                duplicate_rows
            ),
            "duplicate_rows_removed": (
                duplicate_rows
            ),
            "missing_cells_detected": (
                missing_cells
            ),
            "data_quality_score": (
                quality_score
            ),
            "completeness_percentage": (
                _safe_float(
                    profile.get(
                        "completeness_percentage"
                    )
                )
            ),
            "uniqueness_percentage": (
                _safe_float(
                    profile.get(
                        "uniqueness_percentage"
                    )
                )
            ),
            "status_message": (
                "Schema validation, null profiling, "
                "duplicate analysis, and quality scoring "
                "completed using verified dataset values."
            ),
        }


        _log(
            run_id,
            step_id,
            "INFO",
            (
                "Silver validation completed: "
                f"{rows_processed:,} rows retained, "
                f"{duplicate_rows:,} duplicate rows "
                "identified, "
                f"{missing_cells:,} missing cells found, "
                "quality score "
                f"{_format_score(quality_score)}/100."
            ),
            stage="silver",
            rows_received=(
                source_rows
            ),
            rows_processed=(
                rows_processed
            ),
            duplicate_rows=(
                duplicate_rows
            ),
            missing_cells=(
                missing_cells
            ),
            data_quality_score=(
                quality_score
            ),
        )


        return (
            True,
            None,
            rows_processed,
            output,
        )


    if stage == "gold":

        rows_processed = max(
            source_rows
            - duplicate_rows,
            0,
        )


        output = {
            "stage": "gold",
            "rows_processed": (
                rows_processed
            ),
            "business_metrics": (
                business_metrics
            ),
            "business_metric_count": (
                len(
                    business_metrics
                )
            ),
            "status_message": (
                "Verified business outcomes were published "
                "from the processed dataset."
            ),
        }


        metric_names = (
            ", ".join(
                sorted(
                    business_metrics.keys()
                )
            )
            if business_metrics
            else "no standard business KPI fields detected"
        )


        _log(
            run_id,
            step_id,
            "INFO",
            (
                "Gold business-outcome stage completed for "
                f"{rows_processed:,} processed rows; "
                f"available metrics: {metric_names}."
            ),
            stage="gold",
            rows_processed=(
                rows_processed
            ),
            business_metric_count=(
                len(
                    business_metrics
                )
            ),
        )


        return (
            True,
            None,
            rows_processed,
            output,
        )


    rows_processed = max(
        source_rows
        - duplicate_rows,
        0,
    )


    output = {
        "stage": stage,
        "rows_processed": (
            rows_processed
        ),
        "status_message": (
            "Verified local file-processing step completed."
        ),
    }


    _log(
        run_id,
        step_id,
        "INFO",
        (
            f"Verified file-processing step '{step_name}' "
            f"completed for {rows_processed:,} rows."
        ),
        stage=stage,
        rows_processed=(
            rows_processed
        ),
    )


    return (
        True,
        None,
        rows_processed,
        output,
    )


# =========================================================
# TEMPLATE AND CUSTOM EXECUTION
# =========================================================

def _deterministic_rows(
    pipeline_id: str,
    step_id: str,
    position: int,
) -> int:
    """
    Produce stable local demonstration row counts.

    The same pipeline and step produce the same result on
    every run. This avoids random metrics while preserving
    useful monitoring data for pipelines that have no real
    uploaded dataset or external Spark backend.
    """

    seed_text = (
        f"{pipeline_id}:"
        f"{step_id}:"
        f"{position}"
    )


    digest = (
        hashlib
        .sha256(
            seed_text.encode(
                "utf-8"
            )
        )
        .hexdigest()
    )


    numeric_value = int(
        digest[
            :8
        ],
        16,
    )


    return (
        1000
        + (
            numeric_value
            % 49001
        )
    )


def _execute_local_orchestration_step(
    pipeline: Dict[str, Any],
    step: Dict[str, Any],
    run_id: str,
    pipeline_mode: str,
    position: int,
    previous_rows: Optional[int],
) -> Tuple[
    bool,
    Optional[str],
    int,
    Dict[str, Any],
]:
    """
    Execute an enterprise template or custom step through the
    deterministic local orchestration backend.

    This does not claim to execute a real Spark cluster.
    """

    step_id = step[
        "id"
    ]


    step_name = step.get(
        "name",
        step_id,
    )


    step_type = str(
        step.get(
            "step_type",
            "transform",
        )
    )


    engine = str(
        step.get(
            "engine",
            "pyspark",
        )
    )


    _log(
        run_id,
        step_id,
        "INFO",
        (
            f"Starting step '{step_name}' "
            f"({step_type}, {engine}) using the "
            "local orchestration simulation backend."
        ),
        pipeline_mode=(
            pipeline_mode
        ),
        execution_backend=(
            LOCAL_SIMULATION_EXECUTION
        ),
        step_type=(
            step_type
        ),
        engine=(
            engine
        ),
    )


    time.sleep(
        0.2
    )


    if previous_rows is None:

        rows_processed = (
            _deterministic_rows(
                pipeline_id=str(
                    pipeline.get(
                        "id",
                        "pipeline",
                    )
                ),
                step_id=(
                    step_id
                ),
                position=(
                    position
                ),
            )
        )


    else:

        rows_processed = (
            previous_rows
        )


    output = {
        "pipeline_mode": (
            pipeline_mode
        ),
        "execution_backend": (
            LOCAL_SIMULATION_EXECUTION
        ),
        "step_type": (
            step_type
        ),
        "engine": (
            engine
        ),
        "rows_processed": (
            rows_processed
        ),
        "execution_note": (
            "Orchestration behavior was executed locally. "
            "PySpark/SQL infrastructure execution requires "
            "a configured Spark or Databricks backend."
        ),
    }


    _log(
        run_id,
        step_id,
        "INFO",
        (
            f"Step '{step_name}' completed successfully "
            f"with {rows_processed:,} modeled rows. "
            "Execution backend: local orchestration "
            "simulation."
        ),
        pipeline_mode=(
            pipeline_mode
        ),
        execution_backend=(
            LOCAL_SIMULATION_EXECUTION
        ),
        rows_processed=(
            rows_processed
        ),
    )


    return (
        True,
        None,
        rows_processed,
        output,
    )


# =========================================================
# STEP-RUN METADATA
# =========================================================

def _set_step_metadata(
    step_run: Dict[str, Any],
    output: Dict[str, Any],
) -> None:
    """
    Add enterprise execution metadata while preserving all
    existing StepRun fields.
    """

    step_run[
        "execution_details"
    ] = output


    if output.get(
        "stage"
    ):

        step_run[
            "stage"
        ] = output[
            "stage"
        ]


    if output.get(
        "execution_backend"
    ):

        step_run[
            "execution_backend"
        ] = output[
            "execution_backend"
        ]


# =========================================================
# PUBLIC PIPELINE EXECUTION
# =========================================================

def run_pipeline(
    pipeline: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Execute a pipeline according to its creation mode.

    File-driven:
        verified uploaded-data profile execution

    Enterprise template:
        deterministic local orchestration simulation

    Custom:
        deterministic local orchestration simulation
    """

    if not isinstance(
        pipeline,
        dict,
    ):

        raise ValueError(
            "Pipeline configuration must be a dictionary."
        )


    pipeline_id = pipeline.get(
        "id"
    )


    pipeline_name = pipeline.get(
        "name"
    )


    if not pipeline_id:

        raise ValueError(
            "Pipeline ID is required."
        )


    if not pipeline_name:

        raise ValueError(
            "Pipeline name is required."
        )


    pipeline_steps = pipeline.get(
        "steps",
        [],
    )


    if not pipeline_steps:

        raise ValueError(
            "The pipeline does not contain any steps."
        )


    ordered_steps = (
        _topological_order(
            pipeline_steps
        )
    )


    pipeline_mode = (
        _detect_pipeline_mode(
            pipeline
        )
    )


    execution_backend = (
        _execution_backend(
            pipeline_mode
        )
    )


    profile = pipeline.get(
        "dataset_profile",
        {},
    )


    if not isinstance(
        profile,
        dict,
    ):

        profile = {}


    run = Run(
        id=new_id(
            "run"
        ),
        pipeline_id=(
            pipeline_id
        ),
        pipeline_name=(
            pipeline_name
        ),
    )


    run.step_runs = [
        StepRun(
            step_id=step[
                "id"
            ]
        ).__dict__
        for step
        in ordered_steps
    ]


    run_dict = (
        run.to_dict()
    )


    run_dict[
        "pipeline_mode"
    ] = pipeline_mode


    run_dict[
        "execution_backend"
    ] = execution_backend


    run_dict[
        "source_file"
    ] = pipeline.get(
        "source_file"
    )


    if (
        pipeline_mode
        == FILE_DRIVEN_MODE
    ):

        run_dict[
            "dataset_profile"
        ] = profile


        run_dict[
            "business_metrics"
        ] = profile.get(
            "business_metrics",
            {},
        )


        run_dict[
            "rows_read"
        ] = _safe_int(
            profile.get(
                "row_count"
            )
        )


        run_dict[
            "quality_score"
        ] = _safe_float(
            profile.get(
                "data_quality_score"
            )
        )


    store.save_run(
        run_dict
    )


    _log(
        run.id,
        "-",
        "INFO",
        (
            f"Run started for pipeline '{pipeline_name}'. "
            f"Pipeline mode: {pipeline_mode}. "
            f"Execution backend: {execution_backend}."
        ),
        pipeline_mode=(
            pipeline_mode
        ),
        execution_backend=(
            execution_backend
        ),
        source_file=(
            pipeline.get(
                "source_file"
            )
        ),
    )


    failed_upstream = set()


    previous_rows: Optional[
        int
    ] = None


    for position, (
        step,
        step_run,
    ) in enumerate(
        zip(
            ordered_steps,
            run_dict[
                "step_runs"
            ],
        )
    ):

        dependencies = step.get(
            "depends_on",
            [],
        )


        if any(
            dependency
            in failed_upstream
            for dependency
            in dependencies
        ):

            step_run[
                "status"
            ] = "SKIPPED"


            step_run[
                "error_message"
            ] = (
                "Upstream dependency failed."
            )


            step_run[
                "ended_at"
            ] = _now()


            _log(
                run.id,
                step[
                    "id"
                ],
                "WARN",
                (
                    f"Skipping '{step.get('name', step['id'])}': "
                    "an upstream dependency failed."
                ),
                pipeline_mode=(
                    pipeline_mode
                ),
                execution_backend=(
                    execution_backend
                ),
            )


            failed_upstream.add(
                step[
                    "id"
                ]
            )


            store.save_run(
                run_dict
            )


            continue


        step_run[
            "status"
        ] = "RUNNING"


        step_run[
            "started_at"
        ] = _now()


        store.save_run(
            run_dict
        )


        try:

            if (
                pipeline_mode
                == FILE_DRIVEN_MODE
            ):

                (
                    succeeded,
                    error,
                    rows,
                    output,
                ) = (
                    _execute_file_step(
                        step=step,
                        run_id=run.id,
                        profile=profile,
                        position=position,
                    )
                )


            else:

                (
                    succeeded,
                    error,
                    rows,
                    output,
                ) = (
                    _execute_local_orchestration_step(
                        pipeline=(
                            pipeline
                        ),
                        step=(
                            step
                        ),
                        run_id=(
                            run.id
                        ),
                        pipeline_mode=(
                            pipeline_mode
                        ),
                        position=(
                            position
                        ),
                        previous_rows=(
                            previous_rows
                        ),
                    )
                )


        except Exception as error_object:

            succeeded = False


            error = (
                f"{type(error_object).__name__}: "
                f"{error_object}"
            )


            rows = None


            output = {
                "pipeline_mode": (
                    pipeline_mode
                ),
                "execution_backend": (
                    execution_backend
                ),
            }


            _log(
                run.id,
                step[
                    "id"
                ],
                "ERROR",
                (
                    f"Step '{step.get('name', step['id'])}' "
                    f"failed: {error}"
                ),
                pipeline_mode=(
                    pipeline_mode
                ),
                execution_backend=(
                    execution_backend
                ),
            )


        step_run[
            "ended_at"
        ] = _now()


        _set_step_metadata(
            step_run=(
                step_run
            ),
            output=(
                output
            ),
        )


        if succeeded:

            step_run[
                "status"
            ] = "SUCCEEDED"


            step_run[
                "rows_processed"
            ] = rows


            previous_rows = rows


        else:

            step_run[
                "status"
            ] = "FAILED"


            step_run[
                "error_message"
            ] = error


            failed_upstream.add(
                step[
                    "id"
                ]
            )


        store.save_run(
            run_dict
        )


    any_failed = any(
        step_run[
            "status"
        ]
        in (
            "FAILED",
            "SKIPPED",
        )
        for step_run
        in run_dict[
            "step_runs"
        ]
    )


    run_dict[
        "status"
    ] = (
        "FAILED"
        if any_failed
        else "SUCCEEDED"
    )


    run_dict[
        "ended_at"
    ] = _now()


    if (
        pipeline_mode
        == FILE_DRIVEN_MODE
    ):

        source_rows = _safe_int(
            profile.get(
                "row_count"
            )
        )


        duplicate_rows = _safe_int(
            profile.get(
                "duplicate_rows"
            )
        )


        run_dict[
            "rows_read"
        ] = source_rows


        run_dict[
            "rows_processed"
        ] = max(
            source_rows
            - duplicate_rows,
            0,
        )


        run_dict[
            "rows_rejected"
        ] = duplicate_rows


        run_dict[
            "quality_score"
        ] = _safe_float(
            profile.get(
                "data_quality_score"
            )
        )


        run_dict[
            "business_metrics"
        ] = profile.get(
            "business_metrics",
            {},
        )


    else:

        successful_rows = [
            step_run.get(
                "rows_processed"
            )
            for step_run
            in run_dict[
                "step_runs"
            ]
            if step_run.get(
                "status"
            )
            == "SUCCEEDED"
            and step_run.get(
                "rows_processed"
            )
            is not None
        ]


        if successful_rows:

            run_dict[
                "rows_processed"
            ] = successful_rows[
                -1
            ]


    store.save_run(
        run_dict
    )


    _log(
        run.id,
        "-",
        (
            "INFO"
            if run_dict[
                "status"
            ]
            == "SUCCEEDED"
            else "ERROR"
        ),
        (
            f"Run finished with status "
            f"{run_dict['status']}. "
            f"Pipeline mode: {pipeline_mode}. "
            f"Execution backend: {execution_backend}."
        ),
        pipeline_mode=(
            pipeline_mode
        ),
        execution_backend=(
            execution_backend
        ),
        rows_read=(
            run_dict.get(
                "rows_read"
            )
        ),
        rows_processed=(
            run_dict.get(
                "rows_processed"
            )
        ),
        rows_rejected=(
            run_dict.get(
                "rows_rejected"
            )
        ),
        quality_score=(
            run_dict.get(
                "quality_score"
            )
        ),
    )


    return run_dict