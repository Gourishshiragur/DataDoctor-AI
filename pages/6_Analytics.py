"""
DataDoctor AI — Enterprise Pipeline Analytics

Preserves the existing Analytics UI while adding:
- Operational health score
- Run trends
- Status distribution
- Failure-pattern analysis
- Pipeline reliability
- Recovery rate
- Business impact
- Risk classification
- Prioritized recommendations
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from core import store
from core.ui import inject_global_css, sidebar_brand


inject_global_css()
sidebar_brand()

st.title("📈 Analytics")

st.caption(
    "Run trends, reliability, operational health, failure "
    "patterns, and business outcomes across all pipelines."
)


SUCCESS_STATUSES = {
    "SUCCEEDED",
    "SUCCESS",
    "COMPLETED",
}

FAILED_STATUSES = {
    "FAILED",
    "ERROR",
}

REPAIRED_STATUSES = {
    "PARTIALLY_REPAIRED",
    "REPAIRED",
    "RECOVERED",
}


def safe_status(
    run: Dict[str, Any],
) -> str:
    return (
        str(
            run.get(
                "status",
                "UNKNOWN",
            )
        )
        .strip()
        .upper()
    )


def safe_pipeline_name(
    run: Dict[str, Any],
) -> str:
    return (
        str(
            run.get(
                "pipeline_name"
            )
            or run.get(
                "name"
            )
            or "Unnamed pipeline"
        )
        .strip()
    )


def parse_timestamp(
    value: Any,
) -> Optional[datetime]:
    if not value:
        return None

    text = str(
        value
    ).strip()

    if not text:
        return None

    try:
        return datetime.fromisoformat(
            text.replace(
                "Z",
                "+00:00",
            )
        )

    except ValueError:
        pass

    timestamp_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    for timestamp_format in (
        timestamp_formats
    ):
        try:
            return datetime.strptime(
                text,
                timestamp_format,
            )

        except ValueError:
            continue

    return None


def get_run_day(
    run: Dict[str, Any],
) -> str:
    timestamp = (
        run.get(
            "started_at"
        )
        or run.get(
            "created_at"
        )
        or run.get(
            "timestamp"
        )
    )

    parsed = parse_timestamp(
        timestamp
    )

    if parsed:
        return parsed.strftime(
            "%Y-%m-%d"
        )

    text = str(
        timestamp
        or "Unknown"
    )

    return (
        text[:10]
        if len(text) >= 10
        else "Unknown"
    )


def get_step_runs(
    run: Dict[str, Any],
) -> List[dict]:
    step_runs = run.get(
        "step_runs",
        [],
    )

    if not isinstance(
        step_runs,
        list,
    ):
        return []

    return [
        step
        for step in step_runs
        if isinstance(
            step,
            dict,
        )
    ]


def get_error_reason(
    error_message: Any,
) -> str:
    text = str(
        error_message
        or ""
    ).strip()

    if not text:
        return ""

    first_line = (
        text.splitlines()[0]
    )

    if ":" in first_line:
        first_line = (
            first_line.split(
                ":",
                1,
            )[0]
        )

    return (
        first_line[:100]
        or "Unknown failure"
    )


def calculate_health_score(
    total_runs: int,
    successful_runs: int,
    repaired_runs: int,
    failed_runs: int,
) -> float:
    if total_runs <= 0:
        return 0.0

    weighted_success = (
        successful_runs
        + repaired_runs * 0.65
    )

    base_score = (
        weighted_success
        / total_runs
        * 100
    )

    unresolved_penalty = (
        failed_runs
        / total_runs
        * 10
    )

    return round(
        max(
            0.0,
            min(
                100.0,
                base_score
                - unresolved_penalty,
            ),
        ),
        1,
    )


def risk_level(
    success_rate: float,
    failed_runs: int,
) -> str:
    if (
        success_rate >= 95
        and failed_runs == 0
    ):
        return "Low"

    if (
        success_rate >= 80
        and failed_runs <= 2
    ):
        return "Moderate"

    if success_rate >= 60:
        return "High"

    return "Critical"


runs = store.load_runs()


if not runs:
    st.info(
        "No runs yet — create and run a pipeline from "
        "**Pipeline Builder** to see analytics here."
    )

    st.stop()


valid_runs = [
    run
    for run in runs
    if isinstance(
        run,
        dict,
    )
]


if not valid_runs:
    st.info(
        "No valid pipeline-run records are available."
    )

    st.stop()


total = len(
    valid_runs
)

statuses = [
    safe_status(
        run
    )
    for run in valid_runs
]

succeeded = sum(
    status in SUCCESS_STATUSES
    for status in statuses
)

failed = sum(
    status in FAILED_STATUSES
    for status in statuses
)

repaired = sum(
    status in REPAIRED_STATUSES
    for status in statuses
)

success_rate = (
    succeeded
    / total
    * 100
)

recovery_opportunities = (
    failed
    + repaired
)

recovery_rate = (
    repaired
    / recovery_opportunities
    * 100
    if recovery_opportunities
    else 0.0
)

health_score = (
    calculate_health_score(
        total_runs=total,
        successful_runs=succeeded,
        repaired_runs=repaired,
        failed_runs=failed,
    )
)

current_risk = (
    risk_level(
        success_rate,
        failed,
    )
)


c1, c2, c3, c4 = (
    st.columns(4)
)

c1.metric(
    "Total runs",
    total,
)

c2.metric(
    "Success rate",
    f"{success_rate:.0f}%",
)

c3.metric(
    "Failed (unresolved)",
    failed,
)

c4.metric(
    "Repaired",
    repaired,
)


st.divider()


health_1, health_2, health_3 = (
    st.columns(3)
)

health_1.metric(
    "Operational health",
    f"{health_score:.1f}/100",
)

health_2.metric(
    "Recovery rate",
    f"{recovery_rate:.0f}%",
)

health_3.metric(
    "Operational risk",
    current_risk,
)


if health_score >= 90:
    st.success(
        "Pipeline operations are currently healthy. "
        "Continue monitoring reliability and recurring "
        "failure patterns."
    )

elif health_score >= 70:
    st.warning(
        "Pipeline operations are functional but reliability "
        "improvements are recommended."
    )

else:
    st.error(
        "Pipeline reliability requires attention. "
        "Unresolved failures may affect data freshness, "
        "reporting, downstream SLAs, and business decisions."
    )


st.divider()


left, right = (
    st.columns(2)
)


with left:
    st.subheader(
        "Runs over time"
    )

    by_day = Counter(
        get_run_day(
            run
        )
        for run in valid_runs
    )

    trend_rows = [
        {
            "Date": day,
            "Runs": count,
        }
        for day, count in sorted(
            by_day.items()
        )
    ]

    if trend_rows:
        trend_df = pd.DataFrame(
            trend_rows
        )

        trend_chart = px.bar(
            trend_df,
            x="Date",
            y="Runs",
            title=None,
        )

        trend_chart.update_layout(
            xaxis_title="Run date",
            yaxis_title="Number of runs",
            showlegend=False,
            margin=dict(
                l=10,
                r=10,
                t=10,
                b=10,
            ),
        )

        st.plotly_chart(
            trend_chart,
            use_container_width=True,
        )

        known_days = [
            day
            for day in by_day
            if day != "Unknown"
        ]

        if known_days:
            st.caption(
                f"{min(known_days)} → "
                f"{max(known_days)}"
            )


with right:
    st.subheader(
        "Status breakdown"
    )

    status_counts = Counter(
        statuses
    )

    status_df = pd.DataFrame(
        [
            {
                "Status": status,
                "Runs": count,
            }
            for status, count
            in status_counts.items()
        ]
    )

    status_chart = px.bar(
        status_df,
        x="Status",
        y="Runs",
        title=None,
    )

    status_chart.update_layout(
        xaxis_title="Run status",
        yaxis_title="Number of runs",
        showlegend=False,
        margin=dict(
            l=10,
            r=10,
            t=10,
            b=10,
        ),
    )

    st.plotly_chart(
        status_chart,
        use_container_width=True,
    )


st.divider()


st.subheader(
    "Most common failure reasons"
)


error_counter = Counter()


for run in valid_runs:
    for step_run in get_step_runs(
        run
    ):
        error_message = (
            step_run.get(
                "error_message"
            )
        )

        reason = get_error_reason(
            error_message
        )

        if reason:
            error_counter[
                reason
            ] += 1


if not error_counter:
    st.success(
        "No step failures recorded yet."
    )

else:
    failure_rows = [
        {
            "Failure reason": reason,
            "Occurrences": count,
        }
        for reason, count
        in error_counter.most_common(
            8
        )
    ]

    failure_df = pd.DataFrame(
        failure_rows
    )

    failure_chart = px.bar(
        failure_df,
        x="Occurrences",
        y="Failure reason",
        orientation="h",
        title=None,
    )

    failure_chart.update_layout(
        xaxis_title="Occurrences",
        yaxis_title="",
        showlegend=False,
        margin=dict(
            l=10,
            r=10,
            t=10,
            b=10,
        ),
    )

    st.plotly_chart(
        failure_chart,
        use_container_width=True,
    )

    for reason, count in (
        error_counter.most_common(
            8
        )
    ):
        columns = st.columns(
            [4, 1]
        )

        columns[0].write(
            reason
        )

        columns[1].write(
            f"**{count}×**"
        )


st.divider()


st.subheader(
    "Reliability by pipeline"
)


pipeline_names = sorted(
    {
        safe_pipeline_name(
            run
        )
        for run in valid_runs
    }
)


reliability_rows = []


for name in pipeline_names:
    pipeline_runs = [
        run
        for run in valid_runs
        if safe_pipeline_name(
            run
        ) == name
    ]

    pipeline_statuses = [
        safe_status(
            run
        )
        for run in pipeline_runs
    ]

    pipeline_success = sum(
        status in SUCCESS_STATUSES
        for status in pipeline_statuses
    )

    pipeline_repaired = sum(
        status in REPAIRED_STATUSES
        for status in pipeline_statuses
    )

    pipeline_failed = sum(
        status in FAILED_STATUSES
        for status in pipeline_statuses
    )

    pipeline_total = len(
        pipeline_runs
    )

    pipeline_rate = (
        pipeline_success
        / pipeline_total
        * 100
        if pipeline_total
        else 0.0
    )

    reliability_rows.append(
        {
            "Pipeline": name,
            "Runs": pipeline_total,
            "Success rate": (
                pipeline_rate
            ),
            "Succeeded": (
                pipeline_success
            ),
            "Repaired": (
                pipeline_repaired
            ),
            "Failed": (
                pipeline_failed
            ),
        }
    )


reliability_rows.sort(
    key=lambda row: (
        row[
            "Success rate"
        ]
    )
)


for row in reliability_rows:
    st.markdown(
        f"**{row['Pipeline']}** — "
        f"{row['Runs']} runs, "
        f"{row['Success rate']:.0f}% "
        "success rate"
    )

    st.progress(
        max(
            0.0,
            min(
                1.0,
                row[
                    "Success rate"
                ]
                / 100,
            ),
        )
    )

    st.caption(
        f"Succeeded: {row['Succeeded']} · "
        f"Repaired: {row['Repaired']} · "
        f"Failed: {row['Failed']}"
    )


st.divider()


st.subheader(
    "Business outcomes and operational impact"
)


impact_1, impact_2 = (
    st.columns(2)
)


with impact_1:
    st.markdown(
        "**Current operational assessment**"
    )

    if failed > 0:
        st.warning(
            f"{failed} unresolved pipeline run(s) may "
            "affect data freshness, downstream reporting, "
            "analytics availability, and SLA completion."
        )

    elif repaired > 0:
        st.info(
            "Automated or partial recovery reduced the "
            "number of unresolved failures. Repaired runs "
            "should still be reviewed for recurring causes."
        )

    else:
        st.success(
            "No unresolved pipeline failures are currently "
            "recorded."
        )


with impact_2:
    st.markdown(
        "**Decision-support interpretation**"
    )

    if success_rate >= 95:
        st.success(
            "High reliability supports timely downstream "
            "reporting and business decision-making."
        )

    elif success_rate >= 80:
        st.warning(
            "Reliability is acceptable but recurring "
            "failures may create reporting delays or "
            "additional operational effort."
        )

    else:
        st.error(
            "Low reliability may reduce trust in downstream "
            "data products and increase manual recovery "
            "effort."
        )


st.markdown(
    "**Prioritized recommendations**"
)


recommendations = []


if failed > 0:
    recommendations.append(
        (
            "Critical",
            "Investigate unresolved failed runs first and "
            "validate whether downstream datasets or reports "
            "are incomplete.",
        )
    )


if error_counter:
    top_reason, top_count = (
        error_counter.most_common(
            1
        )[0]
    )

    recommendations.append(
        (
            "High",
            f"Create a permanent validation or remediation "
            f"rule for the recurring failure pattern "
            f"'{top_reason}', recorded {top_count} time(s).",
        )
    )


low_reliability = [
    row
    for row in reliability_rows
    if row[
        "Success rate"
    ] < 80
]


if low_reliability:
    recommendations.append(
        (
            "High",
            "Prioritize reliability improvements for: "
            + ", ".join(
                row[
                    "Pipeline"
                ]
                for row
                in low_reliability[
                    :3
                ]
            )
            + ".",
        )
    )


if repaired > 0:
    recommendations.append(
        (
            "Medium",
            "Review repaired runs to confirm data quality, "
            "idempotency, and completeness after recovery.",
        )
    )


recommendations.append(
    (
        "Continuous",
        "Track SLA duration, source-to-target row counts, "
        "data-quality scores, retry counts, and recurring "
        "root causes for production monitoring.",
    )
)


for priority, recommendation in (
    recommendations
):
    st.markdown(
        f"- **{priority}:** "
        f"{recommendation}"
    )


analytics_summary = {
    "total_runs": total,
    "successful_runs": succeeded,
    "failed_runs": failed,
    "repaired_runs": repaired,
    "success_rate": round(
        success_rate,
        2,
    ),
    "recovery_rate": round(
        recovery_rate,
        2,
    ),
    "operational_health_score": (
        health_score
    ),
    "operational_risk": (
        current_risk
    ),
    "top_failure_reasons": (
        error_counter.most_common(
            8
        )
    ),
    "pipeline_reliability": (
        reliability_rows
    ),
}


st.session_state[
    "analysis_results"
] = analytics_summary


try:
    store.save_analysis(
        {
            "id": (
                "pipeline_analytics_latest"
            ),
            "analysis_type": (
                "pipeline_operations"
            ),
            "summary": (
                analytics_summary
            ),
        }
    )

except Exception:
    pass