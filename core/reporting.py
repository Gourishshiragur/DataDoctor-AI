"""
core/reporting.py — Stakeholder Report generation for DataDoctor AI.

Turns a completed run + its dataset profile into:
  1. Modern Plotly graphs (Bronze -> Silver -> Gold funnel, data-quality
     gauge, business KPIs).
  2. A plain-English "embedded answer for stakeholders" — an executive
     summary written by the SAME free-tier LLM chain already used
     elsewhere (Claude API -> Ollama -> deterministic rule-based
     fallback). No new paid dependency is introduced.
  3. An honest repair/self-healing timeline for the run, if any step
     failed and was retried or repaired.

Every number shown here is read directly from the run/profile the caller
passes in — nothing is invented.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import plotly.graph_objects as go


# =========================================================
# STEP-STAGE DETECTION (Bronze / Silver / Gold)
# =========================================================

def _stage_of(step_name: str) -> Optional[str]:
    name = (step_name or "").lower()
    if "bronze" in name:
        return "Bronze"
    if "silver" in name:
        return "Silver"
    if "gold" in name:
        return "Gold"
    return None


def data_flow_sankey(profile: Dict[str, Any], funnel_rows: List[Dict[str, Any]]) -> go.Figure:
    """
    Modern Sankey showing exactly how many real rows moved through each
    stage: Upload -> Schema Detection -> Profiling -> Data Quality ->
    Bronze -> Silver -> Gold. Upstream stages share the uploaded row
    count (no rows are lost before Bronze); Bronze/Silver/Gold widths
    come from the run's actual rows_processed, never invented.
    """
    row_count = int(profile.get("row_count") or 0)
    stage_labels = [
        "Upload", "Schema Detection", "Profiling", "Data Quality",
        "Bronze", "Silver", "Gold",
    ]
    by_stage = {r["stage"]: r["rows_processed"] for r in funnel_rows}
    bronze = by_stage.get("Bronze", row_count)
    silver = by_stage.get("Silver", bronze)
    gold = by_stage.get("Gold", silver)

    values = [row_count, row_count, row_count, bronze, silver, gold]
    colors = ["#3B82F6", "#3B82F6", "#3B82F6", "#F59E0B", "#3B82F6", "#22C55E"]

    fig = go.Figure(go.Sankey(
        node=dict(
            pad=18,
            thickness=16,
            label=stage_labels,
            color=["#64748B", "#64748B", "#64748B", "#64748B", "#F59E0B", "#3B82F6", "#22C55E"],
        ),
        link=dict(
            source=[0, 1, 2, 3, 4, 5],
            target=[1, 2, 3, 4, 5, 6],
            value=values,
            color=colors,
        ),
    ))
    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E2E8F0", size=12),
        title="Data flow — rows moving through each stage",
    )
    return fig


def architecture_summary() -> List[Dict[str, str]]:
    """
    One glanceable, honest list of which real technology powers each
    layer right now — free vs paid — pulled from the actual status
    functions rather than hardcoded marketing copy.
    """
    rows = []
    try:
        from core.llm_provider import llm_status
        ai = llm_status()
        rows.append({
            "layer": "AI (chat / code / repair)",
            "tech": ai["provider"],
            "cost": "Paid (your key)" if ai.get("byok") else ("Paid (deployment key)" if ai["tier"] == 1 else "Free"),
        })
    except Exception:
        pass
    try:
        from core.rag_memory import memory_stats
        rag = memory_stats()
        rows.append({
            "layer": "RAG / Knowledge retrieval",
            "tech": f"{rag['primary_backend']} + {rag['embedding_model']}",
            "cost": "Free (local, no API key)",
        })
    except Exception:
        pass
    try:
        from core.databricks_executor import configured as databricks_configured
        rows.append({
            "layer": "Pipeline execution",
            "tech": "Databricks Free Edition (serverless PySpark)" if databricks_configured() else "Local pandas (verified data or simulation)",
            "cost": "Free" if databricks_configured() else "Free",
        })
    except Exception:
        pass
    return rows


def medallion_funnel(step_runs: List[dict], pipeline_steps: List[dict]) -> List[Dict[str, Any]]:
    """
    Map each step_run to its medallion stage using the pipeline's own step
    names, so the funnel reflects the real dependency chain, not a guess.
    """
    steps_by_id = {s.get("id"): s for s in (pipeline_steps or [])}
    rows = []
    for sr in step_runs or []:
        step = steps_by_id.get(sr.get("step_id"), {})
        stage = _stage_of(step.get("name", "")) or step.get("name", "Step")
        rows.append({
            "stage": stage,
            "rows_processed": sr.get("rows_processed") or 0,
            "status": sr.get("status", "UNKNOWN"),
            "retry_count": sr.get("retry_count", 0),
        })
    return rows


def funnel_chart(funnel_rows: List[Dict[str, Any]]) -> go.Figure:
    """Modern horizontal funnel: rows surviving Bronze -> Silver -> Gold."""
    stages = [r["stage"] for r in funnel_rows] or ["Bronze", "Silver", "Gold"]
    values = [r["rows_processed"] for r in funnel_rows] or [0, 0, 0]
    fig = go.Figure(
        go.Funnel(
            y=stages,
            x=values,
            textinfo="value+percent initial",
            marker={"color": ["#F59E0B", "#3B82F6", "#22C55E"][: len(stages)]},
        )
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E2E8F0"),
        title="Rows surviving each medallion layer",
    )
    return fig


def quality_gauge(data_quality_score: float) -> go.Figure:
    """Modern gauge for the verified data-quality score (0-100)."""
    score = float(data_quality_score or 0)
    color = "#22C55E" if score >= 90 else "#F59E0B" if score >= 70 else "#EF4444"
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=score,
            number={"suffix": "/100"},
            gauge={
                "axis": {"range": [0, 100]},
                "bar": {"color": color},
                "steps": [
                    {"range": [0, 70], "color": "rgba(239,68,68,0.25)"},
                    {"range": [70, 90], "color": "rgba(245,158,11,0.25)"},
                    {"range": [90, 100], "color": "rgba(34,197,94,0.25)"},
                ],
            },
            title={"text": "Data Quality Score"},
        )
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E2E8F0"),
        height=260,
    )
    return fig


def kpi_breakdown_chart(business_metrics: Dict[str, Any]) -> Optional[go.Figure]:
    """
    Bar chart of profit by region or product — whichever verified
    breakdown is present. Returns None if no such KPI was detected
    (never fabricates one).
    """
    breakdown = business_metrics.get("profit_by_region") or business_metrics.get("profit_by_product")
    if not breakdown:
        return None
    label = "region" if "profit_by_region" in business_metrics else "product"
    items = sorted(breakdown.items(), key=lambda kv: kv[1], reverse=True)[:10]
    fig = go.Figure(
        go.Bar(
            x=[k for k, _ in items],
            y=[v for _, v in items],
            marker_color="#7C3AED",
        )
    )
    fig.update_layout(
        margin=dict(l=10, r=10, t=30, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#E2E8F0"),
        title=f"Profit by {label} (top {len(items)})",
    )
    return fig


# =========================================================
# REPAIR / SELF-HEALING TIMELINE
# =========================================================

def repair_timeline(run: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Build an honest step-by-step self-healing timeline from the run's own
    step_runs. Only reports what actually happened — no dramatization.
    """
    timeline = []
    for sr in run.get("step_runs", []) or []:
        retries = sr.get("retry_count", 0) or 0
        status = sr.get("status", "UNKNOWN")
        if retries or status in ("REPAIRED", "PARTIALLY_REPAIRED", "FAILED"):
            timeline.append({
                "step_id": sr.get("step_id"),
                "status": status,
                "retry_count": retries,
                "error_message": sr.get("error_message"),
            })
    return timeline


# =========================================================
# EMBEDDED STAKEHOLDER NARRATIVE (free-tier LLM chain)
# =========================================================

NARRATIVE_SYSTEM_PROMPT = (
    "You are writing a short executive summary of a data pipeline run for a "
    "non-technical business stakeholder. Use plain language, 4-6 sentences, "
    "no jargon, no markdown headers. Lead with the business outcome, then "
    "data quality, then whether anything needed automatic repair. Only use "
    "the numbers given to you — never invent a figure."
)


def _facts_block(pipeline_name: str, profile: Dict[str, Any], run: Dict[str, Any], timeline: List[dict]) -> str:
    metrics = profile.get("business_metrics", {}) or {}
    lines = [
        f"Pipeline: {pipeline_name}",
        f"Run status: {run.get('status')}",
        f"Rows processed: {profile.get('row_count', 'unknown')}",
        f"Columns: {profile.get('column_count', 'unknown')}",
        f"Data quality score: {profile.get('data_quality_score', 'unknown')}/100",
        f"Duplicate rows removed: {profile.get('duplicate_rows', 0)}",
    ]
    if "total_sales" in metrics:
        lines.append(f"Total sales: {metrics['total_sales']:.2f}")
    if "total_profit" in metrics:
        lines.append(f"Total profit: {metrics['total_profit']:.2f}")
    if "profit_rate_percentage" in metrics:
        lines.append(f"Profit rate: {metrics['profit_rate_percentage']:.1f}%")
    if metrics.get("highest_profit_region"):
        r = metrics["highest_profit_region"]
        lines.append(f"Highest-profit region: {r['region']} ({r['profit']:.2f})")
    if metrics.get("highest_profit_product"):
        p = metrics["highest_profit_product"]
        lines.append(f"Highest-profit product: {p['product']} ({p['profit']:.2f})")
    if timeline:
        lines.append(f"Steps that needed repair/retry: {len(timeline)}")
        for t in timeline:
            lines.append(f"  - step {t['step_id']}: {t['status']}, {t['retry_count']} retries")
    else:
        lines.append("No steps required repair or retry.")
    return "\n".join(lines)


def _rule_based_narrative(pipeline_name: str, profile: Dict[str, Any], run: Dict[str, Any], timeline: List[dict]) -> str:
    """Deterministic fallback narrative — used when no LLM tier is available."""
    metrics = profile.get("business_metrics", {}) or {}
    quality = profile.get("data_quality_score", 0)
    rows = profile.get("row_count", 0)
    parts = [
        f"The {pipeline_name} pipeline processed {rows:,} rows and finished with status {run.get('status', 'UNKNOWN')}."
    ]
    if "total_profit" in metrics and "total_sales" in metrics:
        parts.append(
            f"Verified totals: sales of {metrics['total_sales']:,.2f} and profit of {metrics['total_profit']:,.2f}"
            + (f" ({metrics['profit_rate_percentage']:.1f}% profit rate)." if "profit_rate_percentage" in metrics else ".")
        )
    if metrics.get("highest_profit_region"):
        r = metrics["highest_profit_region"]
        parts.append(f"{r['region']} was the highest-profit region at {r['profit']:,.2f}.")
    parts.append(
        f"Data quality scored {quality}/100 based on completeness and uniqueness checks."
    )
    if timeline:
        parts.append(
            f"{len(timeline)} step(s) required automatic retry or repair before the pipeline completed successfully."
        )
    else:
        parts.append("No steps required automatic repair.")
    return " ".join(parts)


def generate_stakeholder_narrative(
    pipeline_name: str,
    profile: Dict[str, Any],
    run: Dict[str, Any],
    timeline: List[dict],
) -> Dict[str, Any]:
    """
    Returns {"text": str, "provider": str, "tier": int}. Tries the real LLM
    chain first (Claude API if a key is configured, else local Ollama);
    falls back to a deterministic template so the report always renders
    something, and always tells the caller which one actually answered.
    """
    facts = _facts_block(pipeline_name, profile, run, timeline)
    try:
        from core.llm_provider import llm_chat
        result = llm_chat(
            prompt=f"Write the executive summary from these verified facts:\n\n{facts}",
            system=NARRATIVE_SYSTEM_PROMPT,
            max_tokens=350,
        )
        # llm_chat's tier 4 is a canned Spark-troubleshooting responder,
        # not a general narrative writer — only trust a REAL model (tier 1-3).
        if result.get("text") and result.get("tier") in (1, 2, 3):
            return {
                "text": result["text"].strip(),
                "provider": result.get("provider", "Unknown"),
                "tier": result.get("tier", 3),
            }
    except Exception:
        pass

    return {
        "text": _rule_based_narrative(pipeline_name, profile, run, timeline),
        "provider": "Internal rule-based engine",
        "tier": 3,
    }
