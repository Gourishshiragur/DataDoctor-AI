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


def _now() -> str:
    """Return the current UTC timestamp."""

    return datetime.now(
        timezone.utc
    ).isoformat()


def _has_retry_history(run: dict) -> bool:
    """
    Return True when at least one pipeline step has been retried.

    Retry history allows the monitoring UI to distinguish a normal
    first-attempt success from a successful recovery.
    """

    return any(
        step_run.get(
            "retry_count",
            0,
        ) > 0
        for step_run in run.get(
            "step_runs",
            []
        )
    )


def retry_step(
    run: dict,
    pipeline: dict,
    step_id: str,
) -> dict:
    """
    Retry one failed pipeline step.

    The run becomes REPAIRED when every step succeeds after the retry.
    """

    step = next(
        step
        for step in pipeline["steps"]
        if step["id"] == step_id
    )

    step_run = next(
        current_step_run
        for current_step_run in run["step_runs"]
        if current_step_run["step_id"] == step_id
    )


    if (
        step_run.get(
            "retry_count",
            0,
        )
        >= MAX_RETRIES_PER_STEP
    ):

        _log(
            run["id"],
            step_id,
            "ERROR",
            (
                f"max retries "
                f"({MAX_RETRIES_PER_STEP}) "
                f"exceeded for "
                f"'{step['name']}'"
            ),
        )

        return run


    # Preserve recovery history at run level.

    run["had_failure"] = True

    run["repair_attempted"] = True


    step_run["retry_count"] = (
        step_run.get(
            "retry_count",
            0,
        )
        + 1
    )

    step_run["status"] = "RUNNING"

    step_run["started_at"] = _now()

    store.save_run(run)


    succeeded, error, rows = (
        _execute_step(
            step,
            run["id"],
        )
    )


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


    if succeeded:

        step_run["rows_processed"] = rows


    run["status"] = (
        _recompute_run_status(
            run
        )
    )


    if run["status"] == "REPAIRED":

        run["repaired_at"] = _now()


        _log(
            run["id"],
            step_id,
            "INFO",
            (
                "step retry recovered the run; "
                "final status REPAIRED"
            ),
        )


    store.save_run(run)

    return run


def repair_run(
    run: dict,
    pipeline: dict,
) -> dict:
    """
    Repair a failed pipeline run.

    FAILED and SKIPPED steps are processed in dependency order.
    Steps that already succeeded are not executed again.
    """

    # Preserve the fact that recovery was required.

    run["had_failure"] = True

    run["repair_attempted"] = True


    ordered_steps = (
        _topological_order(
            pipeline["steps"]
        )
    )


    step_runs_by_id = {

        step_run["step_id"]:
            step_run

        for step_run
        in run["step_runs"]

    }


    for step in ordered_steps:


        step_run = (

            step_runs_by_id[
                step["id"]
            ]

        )


        if step_run["status"] not in (

            "FAILED",
            "SKIPPED",

        ):

            continue


        upstream_ok = all(

            step_runs_by_id[
                dependency_id
            ]["status"]
            == "SUCCEEDED"

            for dependency_id

            in step.get(
                "depends_on",
                [],
            )

        )


        if not upstream_ok:


            step_run["status"] = (

                "SKIPPED"

            )


            step_run["error_message"] = (

                "upstream still failing"

            )


            continue


        if (

            step_run.get(
                "retry_count",
                0,
            )

            >= MAX_RETRIES_PER_STEP

        ):


            _log(

                run["id"],

                step["id"],

                "WARN",

                (
                    f"'{step['name']}' "
                    "left FAILED — "
                    "maximum retries reached"
                ),

            )


            continue


        step_run["retry_count"] = (

            step_run.get(
                "retry_count",
                0,
            )

            + 1

        )


        step_run["status"] = (

            "RUNNING"

        )


        step_run["started_at"] = (

            _now()

        )


        store.save_run(run)


        succeeded, error, rows = (

            _execute_step(

                step,

                run["id"],

            )

        )


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


            step_run[
                "rows_processed"
            ] = rows


    run["status"] = (

        _recompute_run_status(

            run

        )

    )


    run["ended_at"] = (

        _now()

    )


    if run["status"] == "REPAIRED":


        run["repaired_at"] = (

            _now()

        )


    store.save_run(run)


    _log(

        run["id"],

        "-",

        "INFO",

        (
            "repair completed, "
            f"run status now "
            f"{run['status']}"
        ),

    )


    return run


def _recompute_run_status(
    run: dict,
) -> str:
    """
    Calculate the enterprise run status.

    A recovered pipeline is reported as REPAIRED rather than SUCCEEDED
    so normal success is not confused with success after remediation.
    """

    step_runs = (

        run.get(
            "step_runs",
            []
        )

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


    # -----------------------------------------------------
    # ACTIVE EXECUTION
    # -----------------------------------------------------

    if any(

        status == "RUNNING"

        for status

        in statuses

    ):

        return "RUNNING"


    # -----------------------------------------------------
    # COMPLETE SUCCESS
    # -----------------------------------------------------

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


        if recovery_occurred:

            return "REPAIRED"


        return "SUCCEEDED"


    # -----------------------------------------------------
    # UNRESOLVED FAILURE
    # -----------------------------------------------------

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