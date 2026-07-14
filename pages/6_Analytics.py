"""
DataDoctor AI — Enterprise Pipeline Analytics

Features:
- Enterprise operational KPI cards
- Smart operational-health score formatting
- Modern interactive Plotly analytics
- Run-volume trends
- Pipeline-status distribution
- Failure-pattern analysis
- Pipeline reliability comparison
- Recovery effectiveness
- Business impact assessment
- Operational-risk classification
- Prioritized recommendations
"""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from core import store
from core.ui import inject_global_css, sidebar_brand


inject_global_css()
sidebar_brand()

st.title("📈 Enterprise Analytics")

st.caption(
    "Operational intelligence for pipeline reliability, "
    "recovery effectiveness, failure patterns, service risk, "
    "and business outcomes."
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


STATUS_COLORS = {
    "SUCCEEDED": "#22C55E",
    "SUCCESS": "#22C55E",
    "COMPLETED": "#22C55E",
    "REPAIRED": "#38BDF8",
    "RECOVERED": "#38BDF8",
    "PARTIALLY_REPAIRED": "#F59E0B",
    "FAILED": "#EF4444",
    "ERROR": "#EF4444",
    "UNKNOWN": "#94A3B8",
}


PLOT_LAYOUT = {
    "paper_bgcolor": "rgba(0,0,0,0)",
    "plot_bgcolor": "rgba(0,0,0,0)",
    "font": {
        "color": "#CBD5E1",
        "family": "Inter, sans-serif",
    },
    "margin": {
        "l": 20,
        "r": 20,
        "t": 30,
        "b": 20,
    },
    "hoverlabel": {
        "bgcolor": "#111827",
        "font_color": "#F8FAFC",
    },
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

    for timestamp_format in [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]:
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


def format_score(
    score: float,
) -> str:
    """
    Keep useful decimal precision.

    Examples:
    97.8 -> 97.8
    93.0 -> 93
    100.0 -> 100
    """

    numeric_score = float(
        score
    )

    if numeric_score.is_integer():
        return f"{numeric_score:.0f}"

    return (
        f"{numeric_score:.1f}"
        .rstrip("0")
        .rstrip(".")
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


def risk_color(
    risk: str,
) -> str:
    return {
        "Low": "#22C55E",
        "Moderate": "#F59E0B",
        "High": "#F97316",
        "Critical": "#EF4444",
    }.get(
        risk,
        "#94A3B8",
    )


runs = store.load_runs()


if not runs:
    st.info(
        "No runs yet — create and run a pipeline from "
        "**Pipeline Builder** to see enterprise analytics."
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

health_display = (
    format_score(
        health_score
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
    f"{health_display}/100",
)

health_2.metric(
    "Recovery effectiveness",
    f"{recovery_rate:.0f}%",
)

health_3.metric(
    "Operational risk",
    current_risk,
)


if health_score >= 90:
    st.success(
        "Pipeline operations are currently healthy. "
        "Continue monitoring reliability, recovery quality, "
        "and recurring failure patterns."
    )

elif health_score >= 70:
    st.warning(
        "Pipeline operations are functional, but targeted "
        "reliability improvements are recommended."
    )

else:
    st.error(
        "Pipeline reliability requires attention. "
        "Unresolved failures may affect data freshness, "
        "reporting, downstream SLAs, and business decisions."
    )


st.divider()


st.subheader(
    "Operational intelligence"
)

left, right = (
    st.columns(
        [1.55, 1]
    )
)


with left:
    st.markdown(
        "**Pipeline run trend**"
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

    trend_df = pd.DataFrame(
        trend_rows
    )

    trend_chart = go.Figure()

    trend_chart.add_trace(
        go.Scatter(
            x=trend_df[
                "Date"
            ],
            y=trend_df[
                "Runs"
            ],
            mode=(
                "lines+markers"
            ),
            name="Pipeline runs",
            line={
                "width": 4,
                "color": "#38BDF8",
                "shape": "spline",
            },
            marker={
                "size": 10,
                "color": "#A78BFA",
                "line": {
                    "width": 2,
                    "color": "#E0F2FE",
                },
            },
            fill="tozeroy",
            fillcolor=(
                "rgba(56, 189, 248, 0.12)"
            ),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Runs: %{y}"
                "<extra></extra>"
            ),
        )
    )

    trend_chart.update_layout(
        **PLOT_LAYOUT,
        height=360,
        showlegend=False,
        xaxis={
            "title": "",
            "showgrid": False,
            "linecolor": (
                "rgba(148,163,184,0.2)"
            ),
        },
        yaxis={
            "title": "Run volume",
            "rangemode": "tozero",
            "gridcolor": (
                "rgba(148,163,184,0.12)"
            ),
            "zeroline": False,
        },
    )

    st.plotly_chart(
        trend_chart,
        use_container_width=True,
        config={
            "displayModeBar": False,
        },
    )


with right:
    st.markdown(
        "**Run-status distribution**"
    )

    status_counts = Counter(
        statuses
    )

    status_labels = list(
        status_counts.keys()
    )

    status_values = [
        status_counts[
            status
        ]
        for status in status_labels
    ]

    status_chart = go.Figure(
        data=[
            go.Pie(
                labels=(
                    status_labels
                ),
                values=(
                    status_values
                ),
                hole=0.67,
                marker={
                    "colors": [
                        STATUS_COLORS.get(
                            status,
                            "#94A3B8",
                        )
                        for status
                        in status_labels
                    ],
                    "line": {
                        "color": "#0B1220",
                        "width": 3,
                    },
                },
                textinfo=(
                    "label+percent"
                ),
                hovertemplate=(
                    "<b>%{label}</b><br>"
                    "Runs: %{value}<br>"
                    "Share: %{percent}"
                    "<extra></extra>"
                ),
            )
        ]
    )

    status_chart.add_annotation(
        text=(
            f"<b>{total}</b>"
            "<br>runs"
        ),
        x=0.5,
        y=0.5,
        showarrow=False,
        font={
            "size": 22,
            "color": "#F8FAFC",
        },
    )

    status_chart.update_layout(
        **PLOT_LAYOUT,
        height=360,
        showlegend=False,
    )

    st.plotly_chart(
        status_chart,
        use_container_width=True,
        config={
            "displayModeBar": False,
        },
    )


st.divider()


st.subheader(
    "Failure intelligence"
)


error_counter = Counter()


for run in valid_runs:
    for step_run in get_step_runs(
        run
    ):
        reason = get_error_reason(
            step_run.get(
                "error_message"
            )
        )

        if reason:
            error_counter[
                reason
            ] += 1


if not error_counter:
    st.success(
        "No step-level failure patterns are currently "
        "recorded."
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

    failure_df = (
        pd.DataFrame(
            failure_rows
        )
        .sort_values(
            "Occurrences",
            ascending=True,
        )
    )

    failure_chart = px.bar(
        failure_df,
        x="Occurrences",
        y="Failure reason",
        orientation="h",
        text="Occurrences",
    )

    failure_chart.update_traces(
        marker_color="#F97316",
        marker_line_color=(
            "rgba(255,255,255,0.18)"
        ),
        marker_line_width=1,
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Occurrences: %{x}"
            "<extra></extra>"
        ),
    )

    failure_chart.update_layout(
        **PLOT_LAYOUT,
        height=max(
            320,
            len(
                failure_rows
            )
            * 58,
        ),
        showlegend=False,
        xaxis={
            "title": "Occurrences",
            "showgrid": True,
            "gridcolor": (
                "rgba(148,163,184,0.12)"
            ),
        },
        yaxis={
            "title": "",
            "showgrid": False,
        },
    )

    st.plotly_chart(
        failure_chart,
        use_container_width=True,
        config={
            "displayModeBar": False,
        },
    )


st.divider()


st.subheader(
    "Pipeline reliability"
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


reliability_df = (
    pd.DataFrame(
        reliability_rows
    )
    .sort_values(
        "Success rate",
        ascending=True,
    )
)


reliability_chart = go.Figure()


reliability_chart.add_trace(
    go.Bar(
        x=reliability_df[
            "Success rate"
        ],
        y=reliability_df[
            "Pipeline"
        ],
        orientation="h",
        marker={
            "color": reliability_df[
                "Success rate"
            ],
            "colorscale": [
                [0.0, "#EF4444"],
                [0.6, "#F59E0B"],
                [1.0, "#22C55E"],
            ],
            "cmin": 0,
            "cmax": 100,
        },
        text=[
            f"{value:.0f}%"
            for value
            in reliability_df[
                "Success rate"
            ]
        ],
        textposition="outside",
        customdata=(
            reliability_df[
                [
                    "Runs",
                    "Succeeded",
                    "Repaired",
                    "Failed",
                ]
            ]
            .to_numpy()
        ),
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Success rate: %{x:.1f}%<br>"
            "Runs: %{customdata[0]}<br>"
            "Succeeded: %{customdata[1]}<br>"
            "Repaired: %{customdata[2]}<br>"
            "Failed: %{customdata[3]}"
            "<extra></extra>"
        ),
    )
)


reliability_chart.update_layout(
    **PLOT_LAYOUT,
    height=max(
        320,
        len(
            reliability_rows
        )
        * 65,
    ),
    showlegend=False,
    xaxis={
        "title": "Success rate",
        "range": [
            0,
            108,
        ],
        "ticksuffix": "%",
        "gridcolor": (
            "rgba(148,163,184,0.12)"
        ),
    },
    yaxis={
        "title": "",
        "showgrid": False,
    },
)


st.plotly_chart(
    reliability_chart,
    use_container_width=True,
    config={
        "displayModeBar": False,
    },
)


st.divider()


st.subheader(
    "Business outcomes and operational impact"
)


impact_1, impact_2, impact_3 = (
    st.columns(3)
)


with impact_1:
    st.markdown(
        "#### Data availability"
    )

    if failed > 0:
        st.error(
            f"{failed} unresolved run(s) may delay data "
            "availability for reporting, analytics, or "
            "downstream consumers."
        )

    elif repaired > 0:
        st.info(
            "Recovery reduced unresolved disruption, but "
            "repaired outputs should be validated for "
            "completeness and consistency."
        )

    else:
        st.success(
            "No unresolved run failures are currently "
            "recorded."
        )


with impact_2:
    st.markdown(
        "#### Operational efficiency"
    )

    if repaired > 0:
        st.success(
            "Automated or assisted recovery reduced manual "
            "intervention and prevented the repaired runs "
            "from remaining unresolved."
        )

    else:
        st.info(
            "No recovery events are currently recorded. "
            "Continue measuring retry effort and manual "
            "intervention."
        )


with impact_3:
    st.markdown(
        "#### Decision confidence"
    )

    if success_rate >= 95:
        st.success(
            "High run reliability supports timely reporting "
            "and stronger confidence in downstream data."
        )

    elif success_rate >= 80:
        st.warning(
            "Current reliability is usable, but recurring "
            "issues may create reporting delays or extra "
            "operational effort."
        )

    else:
        st.error(
            "Low reliability may reduce trust in downstream "
            "data products and business decisions."
        )


st.markdown(
    "### Prioritized recommendations"
)


recommendations = []


if failed > 0:
    recommendations.append(
        (
            "Critical",
            "Investigate unresolved runs and validate whether "
            "downstream datasets, reports, or SLA deliveries "
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
            "Create a permanent validation or remediation "
            f"rule for '{top_reason}', which occurred "
            f"{top_count} time(s).",
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
            "Prioritize reliability improvements for "
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
            "Validate repaired runs for data completeness, "
            "quality, idempotency, and downstream consistency.",
        )
    )


recommendations.append(
    (
        "Continuous",
        "Track SLA duration, source-to-target row counts, "
        "data-quality scores, retry counts, recovery time, "
        "and recurring root causes.",
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