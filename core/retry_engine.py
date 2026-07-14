"""
Retry and repair logic for failed pipeline runs.

Operational modes:

- retry_step():
  Re-runs one failed step for transient failures such as executor loss,
  timeout, temporary storage issues, or Delta concurrency conflicts.

- repair_run():
  Re-runs FAILED and SKIPPED steps in dependency order while preserving
  already successful steps.

Enterprise run-status behavior:

- SUCCEEDED:
  Every step succeeded during normal execution without recovery.

- REPAIRED:
  One or more steps previously failed or were skipped, but all steps
  succeeded after retry or repair.

- PARTIALLY_REPAIRED:
  Recovery succeeded for some steps, but failed or skipped steps remain.

- FAILED:
  Failed or skipped steps remain and recovery has not produced a partial
  recovery.

- RUNNING:
  One or more steps are still executing.
"""

from datetime import datetime, timezone

from core import store
from core.pipeline_engine import (
    _execute_step,
    _log,
    _topological_order,
)


MAX_RETRIES_PER_STEP = 3


# ============================================================
# General utilities
# ============================================================

def _now() -> str:
    """
    Return the current timezone-aware UTC timestamp.
    """

    return datetime.now(
        timezone.utc
    ).isoformat()


def _has_retry_history(
    run: dict,
) -> bool:
    """
    Return True when at least one pipeline step has been
    retried.

    Retry history allows the monitoring UI to distinguish a
    normal first-attempt success from successful recovery.
    """

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
    """
    Find a pipeline step using its ID.

    A clear ValueError is raised instead of allowing an
    unhelpful StopIteration exception.
    """

    if not isinstance(
        pipeline,
        dict,
    ):
        raise TypeError(
            "pipeline must be a dictionary."
        )

    for step in pipeline.get(
        "steps",
        [],
    ):

        if (
            isinstance(
                step,
                dict,
            )
            and step.get(
                "id"
            ) == step_id
        ):

            return step

    raise ValueError(
        (
            "Pipeline step was not found: "
            f"'{step_id}'."
        )
    )


def _find_step_run(
    run: dict,
    step_id: str,
) -> dict:
    """
    Find execution state for one pipeline step.

    A clear ValueError is raised when the run does not contain
    the requested step.
    """

    if not isinstance(
        run,
        dict,
    ):
        raise TypeError(
            "run must be a dictionary."
        )

    for step_run in run.get(
        "step_runs",
        [],
    ):

        if (
            isinstance(
                step_run,
                dict,
            )
            and step_run.get(
                "step_id"
            ) == step_id
        ):

            return step_run

    raise ValueError(
        (
            "Pipeline step execution was not found: "
            f"'{step_id}'."
        )
    )


# ============================================================
# Individual step retry
# ============================================================

def retry_step(
    run: dict,
    pipeline: dict,
    step_id: str,
) -> dict:
    """
    Retry one failed pipeline step.

    Only FAILED steps may be retried.

    The run becomes REPAIRED when every pipeline step succeeds
    after retry.
    """

    step = _find_pipeline_step(
        pipeline,
        step_id,
    )

    step_run = _find_step_run(
        run,
        step_id,
    )


    # --------------------------------------------------------
    # Validate retry state
    # --------------------------------------------------------

    current_status = step_run.get(
        "status",
        "UNKNOWN",
    )


    if current_status != "FAILED":

        raise ValueError(
            (
                f"Step '{step.get('name', step_id)}' "
                "cannot be retried because its current "
                f"status is '{current_status}'. "
                "Only FAILED steps can be retried."
            )
        )


    retry_count = step_run.get(
        "retry_count",
        0,
    )


    # --------------------------------------------------------
    # Enforce maximum retry limit
    # --------------------------------------------------------

    if (
        retry_count
        >= MAX_RETRIES_PER_STEP
    ):

        _log(
            run["id"],
            step_id,
            "ERROR",
            (
                "Maximum retries "
                f"({MAX_RETRIES_PER_STEP}) "
                "reached for "
                f"'{step.get('name', step_id)}'."
            ),
        )

        return run


    # --------------------------------------------------------
    # Preserve recovery history
    # --------------------------------------------------------

    run["had_failure"] = True

    run["repair_attempted"] = True


    # --------------------------------------------------------
    # Start retry
    # --------------------------------------------------------

    step_run["retry_count"] = (
        retry_count
        + 1
    )

    step_run["status"] = (
        "RUNNING"
    )

    step_run["started_at"] = (
        _now()
    )

    step_run["ended_at"] = None

    step_run["error_message"] = None


    run["status"] = (
        "RUNNING"
    )


    store.save_run(
        run
    )


    _log(
        run["id"],
        step_id,
        "INFO",
        (
            f"Retry attempt "
            f"{step_run['retry_count']}/"
            f"{MAX_RETRIES_PER_STEP} "
            "started for "
            f"'{step.get('name', step_id)}'."
        ),
    )


    # --------------------------------------------------------
    # Execute failed step
    # --------------------------------------------------------

    succeeded, error, rows = (
        _execute_step(
            step,
            run["id"],
        )
    )


    # --------------------------------------------------------
    # Save retry result
    # --------------------------------------------------------

    step_run["ended_at"] = (
        _now()
    )

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


    if succeeded:

        step_run["rows_processed"] = (
            rows
        )


    run["status"] = (
        _recompute_run_status(
            run
        )
    )


    if (
        run["status"]
        == "REPAIRED"
    ):

        run["repaired_at"] = (
            _now()
        )

        run["ended_at"] = (
            _now()
        )


        _log(
            run["id"],
            step_id,
            "INFO",
            (
                "Step retry recovered the run; "
                "final status is REPAIRED."
            ),
        )


    elif succeeded:

        _log(
            run["id"],
            step_id,
            "INFO",
            (
                "Step retry succeeded; "
                f"run status is {run['status']}."
            ),
        )


    else:

        _log(
            run["id"],
            step_id,
            "ERROR",
            (
                "Step retry failed; "
                f"run status is {run['status']}."
            ),
        )


    store.save_run(
        run
    )


    return run


# ============================================================
# Complete run repair
# ============================================================

def repair_run(
    run: dict,
    pipeline: dict,
) -> dict:
    """
    Repair a failed pipeline run.

    FAILED and SKIPPED steps are processed in dependency order.

    Steps that already succeeded are preserved and are not
    executed again.
    """

    if not isinstance(
        run,
        dict,
    ):
        raise TypeError(
            "run must be a dictionary."
        )


    if not isinstance(
        pipeline,
        dict,
    ):
        raise TypeError(
            "pipeline must be a dictionary."
        )


    # --------------------------------------------------------
    # Preserve recovery history
    # --------------------------------------------------------

    run["had_failure"] = True

    run["repair_attempted"] = True


    ordered_steps = (
        _topological_order(
            pipeline.get(
                "steps",
                [],
            )
        )
    )


    step_runs_by_id = {

        step_run["step_id"]:
            step_run

        for step_run
        in run.get(
            "step_runs",
            [],
        )

        if isinstance(
            step_run,
            dict,
        )

        and step_run.get(
            "step_id"
        )

    }


    # --------------------------------------------------------
    # Process failed and skipped steps
    # --------------------------------------------------------

    for step in ordered_steps:


        step_id = step.get(
            "id"
        )


        if (
            step_id
            not in step_runs_by_id
        ):

            _log(
                run.get(
                    "id",
                    "unknown-run",
                ),
                step_id or "-",
                "WARN",
                (
                    "Repair skipped pipeline step "
                    "because no matching step-run "
                    "record was found."
                ),
            )

            continue


        step_run = (
            step_runs_by_id[
                step_id
            ]
        )


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


        # ----------------------------------------------------
        # Validate upstream dependencies
        # ----------------------------------------------------

        upstream_ok = True


        for dependency_id in (
            step.get(
                "depends_on",
                [],
            )
        ):


            dependency_run = (
                step_runs_by_id.get(
                    dependency_id
                )
            )


            if (
                dependency_run
                is None
                or dependency_run.get(
                    "status"
                )
                != "SUCCEEDED"
            ):

                upstream_ok = False

                break


        if not upstream_ok:

            step_run["status"] = (
                "SKIPPED"
            )

            step_run["error_message"] = (
                "Upstream dependency is still failing."
            )

            continue


        # ----------------------------------------------------
        # Enforce retry limit
        # ----------------------------------------------------

        retry_count = step_run.get(
            "retry_count",
            0,
        )


        if (
            retry_count
            >= MAX_RETRIES_PER_STEP
        ):

            if (
                step_run.get(
                    "status"
                )
                == "SKIPPED"
            ):

                step_run["status"] = (
                    "FAILED"
                )


            step_run["error_message"] = (
                "Maximum retry limit reached."
            )


            _log(
                run["id"],
                step_id,
                "WARN",
                (
                    f"'{step.get('name', step_id)}' "
                    "was not retried because the "
                    "maximum retry limit was reached."
                ),
            )

            continue


        # ----------------------------------------------------
        # Start repair attempt
        # ----------------------------------------------------

        step_run["retry_count"] = (
            retry_count
            + 1
        )

        step_run["status"] = (
            "RUNNING"
        )

        step_run["started_at"] = (
            _now()
        )

        step_run["ended_at"] = None

        step_run["error_message"] = None


        run["status"] = (
            "RUNNING"
        )


        store.save_run(
            run
        )


        _log(
            run["id"],
            step_id,
            "INFO",
            (
                f"Repair attempt "
                f"{step_run['retry_count']}/"
                f"{MAX_RETRIES_PER_STEP} "
                "started for "
                f"'{step.get('name', step_id)}'."
            ),
        )


        # ----------------------------------------------------
        # Execute step
        # ----------------------------------------------------

        succeeded, error, rows = (
            _execute_step(
                step,
                run["id"],
            )
        )


        # ----------------------------------------------------
        # Save execution result
        # ----------------------------------------------------

        step_run["ended_at"] = (
            _now()
        )

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


        if succeeded:

            step_run["rows_processed"] = (
                rows
            )


        store.save_run(
            run
        )


    # --------------------------------------------------------
    # Calculate final run status
    # --------------------------------------------------------

    run["status"] = (
        _recompute_run_status(
            run
        )
    )


    run["ended_at"] = (
        _now()
    )


    if (
        run["status"]
        == "REPAIRED"
    ):

        run["repaired_at"] = (
            _now()
        )


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


# ============================================================
# Enterprise run-status calculation
# ============================================================

def _recompute_run_status(
    run: dict,
) -> str:
    """
    Calculate the enterprise pipeline-run status.

    A recovered pipeline is reported as REPAIRED rather than
    SUCCEEDED so normal first-attempt success is not confused
    with successful automated remediation.
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


    # --------------------------------------------------------
    # Active execution
    # --------------------------------------------------------

    if any(

        status
        == "RUNNING"

        for status
        in statuses

    ):

        return "RUNNING"


    # --------------------------------------------------------
    # Complete success
    # --------------------------------------------------------

    if all(

        status
        == "SUCCEEDED"

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


        if recovery_occurred:

            return "REPAIRED"


        return "SUCCEEDED"


    # --------------------------------------------------------
    # Unresolved failure
    # --------------------------------------------------------

    unresolved_failure = any(

        status in (
            "FAILED",
            "SKIPPED",
        )

        for status
        in statuses

    )


    successful_steps = any(

        status
        == "SUCCEEDED"

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

        return (
            "PARTIALLY_REPAIRED"
        )


    if unresolved_failure:

        return "FAILED"


    return "RUNNING"