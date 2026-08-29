"""pipeline/replay.py — replays a historical pipeline run step-by-step for the Replay page,
using the events/quality_checks/repairs already logged in database/history.py.

This does not re-execute the pipeline; it reconstructs the *narrative* of what happened
during a past run, in order, for auditing and demoing "self-healing" behavior after the fact.
"""
from __future__ import annotations

from database import history


def get_run_timeline(run_id: str) -> list[dict]:
    """Merge events, quality checks, and repairs into one time-ordered timeline."""
    run = history.get_run(run_id)
    if not run:
        return []

    timeline = []
    for e in history.get_events(run_id):
        timeline.append({"ts": e["ts"], "type": "event", "stage": e["stage"], "detail": e["message"]})

    for r in history.get_repairs(run_id):
        timeline.append({
            "ts": run["started_at"],  # repairs table doesn't store ts separately in this view
            "type": "repair",
            "stage": "silver",
            "detail": f"{r['column_name']}: {r['issue']} -> {r['action']} ({r['rows_affected']} rows)",
        })

    timeline.sort(key=lambda x: x["ts"])
    return timeline


def list_runs(dataset: str | None = None, limit: int = 50) -> list[dict]:
    runs = history.get_runs(limit=limit)
    if dataset:
        runs = [r for r in runs if r["dataset"] == dataset]
    return runs
