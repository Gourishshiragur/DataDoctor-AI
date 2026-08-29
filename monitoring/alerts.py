"""monitoring/alerts.py — derives alert-worthy conditions from recent run history,
for display on the Monitor page. Pure functions over database/history.py data —
no external alerting service required (structured so Slack/email/webhook senders
could be plugged in later without touching callers)."""
from __future__ import annotations

from database import history


def get_active_alerts(limit_runs: int = 10) -> list[dict]:
    alerts = []
    runs = history.get_runs(limit=limit_runs)
    for run in runs:
        checks = history.get_quality_checks(run["run_id"])
        failed = [c for c in checks if not c["passed"] and c["layer"] == "silver"]
        for f in failed:
            alerts.append({
                "run_id": run["run_id"],
                "dataset": run["dataset"],
                "severity": "warning",
                "message": f"Quality check '{f['check_name']}' still failing after self-healing on Silver layer.",
                "details": f["details"],
            })
        if run["status"] == "failed":
            alerts.append({
                "run_id": run["run_id"],
                "dataset": run["dataset"],
                "severity": "error",
                "message": "Pipeline run failed.",
                "details": {},
            })
    return alerts


def health_summary() -> dict:
    runs = history.get_runs(limit=50)
    total = len(runs)
    succeeded = len([r for r in runs if r["status"] == "success"])
    failed = len([r for r in runs if r["status"] == "failed"])
    return {
        "total_runs": total,
        "succeeded": succeeded,
        "failed": failed,
        "success_rate": round((succeeded / total * 100), 1) if total else 100.0,
    }
