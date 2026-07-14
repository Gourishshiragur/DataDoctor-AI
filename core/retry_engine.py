"""
Retry and repair logic for DataDoctor AI pipeline runs.

Supports:
- File-driven Bronze → Silver → Gold pipelines
- Enterprise template pipelines
- Custom pipelines
- Individual failed-step retry
- Dependency-aware full-run repair
- Maximum retry limits
- REPAIRED and PARTIALLY_REPAIRED statuses
"""

from datetime import datetime, timezone

from core import store
from core.pipeline_engine import (
    FILE_DRIVEN_MODE,
    _detect_pipeline_mode,
    _execute_file_step,
    _execute_local_orchestration_step,
    _execution_backend,
    _log,
    _set_step_metadata,
    _topological_order,
)


MAX_RETRIES_PER_STEP = 3


def _now() -> str:
    """Return the current timezone-aware UTC timestamp."""

    return datetime.now(
        timezone.utc
    ).isoformat()


def _has_retry_history(
    run: dict,
) -> bool:
    """Return True when at least one step has been retried."""

    return any(
        step_run.get(
            "retry_count",
            0,
        ) > 0
        for step_run
        in run.get(
            "step_runs",
            [],
        )
    )


def _find_pipeline_step(
    pipeline: dict,
    step_id: str,
) -> dict:
    """Find a pipeline step using its ID."""

    for step in pipeline.get(
        "steps",
        [],
    ):

        if step.get(
            "id"
        ) == step_id:

            return step

    raise ValueError(
        f"Pipeline step was not found: '{step_id}'."
    )


def _find_step_run(
    run: dict,
    step_id: str,
) -> dict:
    """Find the run record for one pipeline step."""

    for step_run in run.get(
        "step_runs",
        [],
    ):

        if step_run.get(
            "step_id"
        ) == step_id:

            return step_run

    raise ValueError(
        (
            "Pipeline step execution was not found: "
            f"'{step_id}'."
        )
    )


def _get_step_position(
    pipeline: dict,
    step_id: str,
) -> int:
    """Return the dependency-ordered position of a step."""

    ordered_steps = _topological_order(
        pipeline.get(
            "steps",
            [],
        )
    )

    for position, step in enumerate(
        ordered_steps
    ):

        if step.get(
            "id"
        ) == step_id:

            return position

    raise ValueError(
        f"Pipeline step position was not found: '{step_id}'."
    )


def _get_previous_rows(
    run: dict,
    pipeline: dict,
    step_id: str,
):
    """
    Return the latest successful upstream row count for
    template and custom pipeline simulation.
    """

    ordered_steps = _topological_order(
        pipeline.get(
            "steps",
            [],
        )
    )

    step_runs_by_id = {
        step_run.get(
            "step_id"
        ): step_run
        for step_run
        in run.get(
            "step_runs",
            [],
        )
    }

    previous_rows = None

    for step in ordered_steps:

        if step.get(
            "id"
        ) == step_id:

            break

        step_run = step_runs_by_id.get(
            step.get(
                "id"
            ),
            {},
        )

        if (
            step_run.get(
                "status"
            )
            == "SUCCEEDED"
            and step_run.get(
                "rows_processed"
            )
            is not None
        ):

            previous_rows = step_run.get(
                "rows_processed"
            )

    return previous_rows


def _execute_retry_step(
    run: dict,
    pipeline: dict,
    step: dict,
):
    """
    Execute a retry using the same backend as the original
    pipeline run.
    """

    pipeline_mode = (
        run.get(
            "pipeline_mode"
        )
        or _detect_pipeline_mode(
            pipeline
        )
    )

    position = _get_step_position(
        pipeline,
        step["id"],
    )

    if (
        pipeline_mode
        == FILE_DRIVEN_MODE
    ):

        profile = (
            pipeline.get(
                "dataset_profile"
            )
            or run.get(
                "dataset_profile"
            )
            or {}
        )

        return _execute_file_step(
            step=step,
            run_id=run["id"],
            profile=profile,
            position=position,
        )

    previous_rows = _get_previous_rows(
        run=run,
        pipeline=pipeline,
        step_id=step["id"],
    )

    return _execute_local_orchestration_step(
        pipeline=pipeline,
        step=step,
        run_id=run["id"],
        pipeline_mode=pipeline_mode,
        position=position,
        previous_rows=previous_rows,
    )


def retry_step(
    run: dict,
    pipeline: dict,
    step_id: str,
) -> dict:
    """
    Retry one FAILED pipeline step.

    When every pipeline step succeeds after retry, the final
    run status becomes REPAIRED.
    """

    step = _find_pipeline_step(
        pipeline,
        step_id,
    )

    step_run = _find_step_run(
        run,
        step_id,
    )

    current_status = step_run.get(
        "status",
        "UNKNOWN",
    )

    if current_status != "FAILED":

        raise ValueError(
            (
                f"Step '{step.get('name', step_id)}' "
                "cannot be retried because its status is "
                f"'{current_status}'. Only FAILED steps "
                "can be retried."
            )
        )

    retry_count = step_run.get(
        "retry_count",
        0,
    )

    if (
        retry_count
        >= MAX_RETRIES_PER_STEP
    ):

        _log(
            run["id"],
            step_id,
            "ERROR",
            (
                f"Maximum retries "
                f"({MAX_RETRIES_PER_STEP}) reached for "
                f"'{step.get('name', step_id)}'."
            ),
        )

        return run

    run["had_failure"] = True
    run["repair_attempted"] = True
    run["status"] = "RUNNING"

    step_run["retry_count"] = (
        retry_count
        + 1
    )

    step_run["status"] = "RUNNING"
    step_run["started_at"] = _now()
    step_run["ended_at"] = None
    step_run["error_message"] = None

    store.save_run(
        run
    )

    try:

        (
            succeeded,
            error,
            rows,
            output,
        ) = _execute_retry_step(
            run=run,
            pipeline=pipeline,
            step=step,
        )

    except Exception as error_object:

        succeeded = False

        error = (
            f"{type(error_object).__name__}: "
            f"{error_object}"
        )

        rows = None

        output = {
            "execution_backend": (
                run.get(
                    "execution_backend"
                )
                or _execution_backend(
                    _detect_pipeline_mode(
                        pipeline
                    )
                )
            )
        }

    step_run["ended_at"] = _now()

    step_run["status"] = (
        "SUCCEEDED"
        if succeeded
        else "FAILED"
    )

    step_run["error_message"] = (
        None
        if succeeded
        else error
    )

    _set_step_metadata(
        step_run,
        output,
    )

    if succeeded:

        step_run["rows_processed"] = rows

    run["status"] = _recompute_run_status(
        run
    )

    if (
        run["status"]
        == "REPAIRED"
    ):

        run["repaired_at"] = _now()
        run["ended_at"] = _now()

    store.save_run(
        run
    )

    _log(
        run["id"],
        step_id,
        (
            "INFO"
            if succeeded
            else "ERROR"
        ),
        (
            f"Retry completed for "
            f"'{step.get('name', step_id)}'; "
            f"run status is {run['status']}."
        ),
    )

    return run


def repair_run(
    run: dict,
    pipeline: dict,
) -> dict:
    """
    Repair FAILED and SKIPPED steps in dependency order.

    Successful steps are preserved and are not executed again.
    """

    run["had_failure"] = True
    run["repair_attempted"] = True

    ordered_steps = _topological_order(
        pipeline.get(
            "steps",
            [],
        )
    )

    step_runs_by_id = {
        step_run["step_id"]: step_run
        for step_run
        in run.get(
            "step_runs",
            [],
        )
    }

    for step in ordered_steps:

        step_id = step["id"]

        step_run = step_runs_by_id.get(
            step_id
        )

        if step_run is None:

            continue

        if (
            step_run.get(
                "status"
            )
            not in (
                "FAILED",
                "SKIPPED",
            )
        ):

            continue

        upstream_ok = all(
            step_runs_by_id.get(
                dependency_id,
                {},
            ).get(
                "status"
            )
            == "SUCCEEDED"
            for dependency_id
            in step.get(
                "depends_on",
                [],
            )
        )

        if not upstream_ok:

            step_run["status"] = "SKIPPED"

            step_run["error_message"] = (
                "Upstream dependency is still failing."
            )

            continue

        retry_count = step_run.get(
            "retry_count",
            0,
        )

        if (
            retry_count
            >= MAX_RETRIES_PER_STEP
        ):

            step_run["status"] = "FAILED"

            step_run["error_message"] = (
                "Maximum retry limit reached."
            )

            continue

        step_run["retry_count"] = (
            retry_count
            + 1
        )

        step_run["status"] = "RUNNING"
        step_run["started_at"] = _now()
        step_run["ended_at"] = None
        step_run["error_message"] = None

        run["status"] = "RUNNING"

        store.save_run(
            run
        )

        try:

            (
                succeeded,
                error,
                rows,
                output,
            ) = _execute_retry_step(
                run=run,
                pipeline=pipeline,
                step=step,
            )

        except Exception as error_object:

            succeeded = False

            error = (
                f"{type(error_object).__name__}: "
                f"{error_object}"
            )

            rows = None

            output = {}

        step_run["ended_at"] = _now()

        step_run["status"] = (
            "SUCCEEDED"
            if succeeded
            else "FAILED"
        )

        step_run["error_message"] = (
            None
            if succeeded
            else error
        )

        _set_step_metadata(
            step_run,
            output,
        )

        if succeeded:

            step_run["rows_processed"] = rows

        store.save_run(
            run
        )

    run["status"] = _recompute_run_status(
        run
    )

    run["ended_at"] = _now()

    if (
        run["status"]
        == "REPAIRED"
    ):

        run["repaired_at"] = _now()

    store.save_run(
        run
    )

    _log(
        run["id"],
        "-",
        "INFO",
        (
            "Repair completed; "
            f"run status is {run['status']}."
        ),
    )

    return run


def _recompute_run_status(
    run: dict,
) -> str:
    """
    Calculate the final enterprise run status.
    """

    step_runs = run.get(
        "step_runs",
        [],
    )

    if not step_runs:

        return "RUNNING"

    statuses = [
        step_run.get(
            "status",
            "PENDING",
        )
        for step_run
        in step_runs
    ]

    if any(
        status == "RUNNING"
        for status
        in statuses
    ):

        return "RUNNING"

    if all(
        status == "SUCCEEDED"
        for status
        in statuses
    ):

        recovery_occurred = (
            run.get(
                "had_failure",
                False,
            )
            or run.get(
                "repair_attempted",
                False,
            )
            or _has_retry_history(
                run
            )
        )

        return (
            "REPAIRED"
            if recovery_occurred
            else "SUCCEEDED"
        )

    unresolved_failure = any(
        status in (
            "FAILED",
            "SKIPPED",
        )
        for status
        in statuses
    )

    successful_steps = any(
        status == "SUCCEEDED"
        for status
        in statuses
    )

    recovery_attempted = (
        run.get(
            "repair_attempted",
            False,
        )
        or _has_retry_history(
            run
        )
    )

    if (
        unresolved_failure
        and successful_steps
        and recovery_attempted
    ):

        return "PARTIALLY_REPAIRED"

    if unresolved_failure:

        return "FAILED"

    return "RUNNING"