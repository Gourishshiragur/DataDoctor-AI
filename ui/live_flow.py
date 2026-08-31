"""Runtime-driven enterprise pipeline flow."""

from __future__ import annotations

import html
import streamlit as st

from pipeline import runtime_state

LABELS = {
    "source": "SOURCE",
    "bronze": "BRONZE",
    "profiling": "PROFILING",
    "quality": "QUALITY",
    "repair": "AI REPAIR",
    "silver": "SILVER",
    "gold": "GOLD",
}


ACTIVE = {"running", "repairing", "retrying"}


def _css_state(state: str) -> str:
    if state in ACTIVE:
        return "active"
    if state == "success":
        return "complete"
    if state == "failed":
        return "failed"
    if state == "cancelled":
        return "cancelled"
    return "waiting"


def _packets(info: dict) -> str:
    if info.get("state") not in ACTIVE:
        return ""

    rows = int(info.get("rows_in") or info.get("rows_out") or 0)
    count = min(7, max(3, rows // 1000 + 3)) if rows else 3

    return (
        '<div class="dd-packets">' + "".join("<i></i>" for _ in range(count)) + "</div>"
    )


def render(run_id: str | None = None):
    run = runtime_state.get_run(run_id) if run_id else None

    if not run:
        st.html(
            """
            <div class="dd-live">
                <div class="dd-live-title">Live Pipeline Execution</div>
                <div class="dd-live-sub">Waiting for an active run</div>
            </div>
            """,
        )
        return

    stages = run.get("stages", {})

    active_stage = next(
        (
            stage
            for stage in runtime_state.STAGES
            if stages.get(stage, {}).get("state") in ACTIVE
        ),
        None,
    )

    cards = []

    for index, stage in enumerate(runtime_state.STAGES):
        info = stages.get(stage, {})
        state = info.get("state", "waiting")
        css = _css_state(state)

        rows_in = int(info.get("rows_in") or 0)
        rows_out = int(info.get("rows_out") or 0)

        if rows_out:
            rows = f"{rows_out:,} rows out"
        elif rows_in:
            rows = f"{rows_in:,} rows in"
        else:
            rows = "Awaiting data"

        message = html.escape(str(info.get("message") or ""))[:65]

        cards.append(f"""
            <div class="dd-stage {css}">
                <div class="dd-stage-name">{LABELS[stage]}</div>
                <div class="dd-stage-state">{html.escape(str(state).upper())}</div>
                {_packets(info)}
                <div class="dd-stage-rows">{rows}</div>
                <div class="dd-stage-message">{message}</div>
            </div>
            """)

        if index < len(runtime_state.STAGES) - 1:
            next_stage = runtime_state.STAGES[index + 1]
            next_state = stages.get(next_stage, {}).get("state")

            connection = (
                "flowing"
                if state == "success" and next_state in ACTIVE
                else "failed" if next_state == "failed" else ""
            )

            cards.append(f'<div class="dd-connector {connection}"></div>')

    metrics = run.get("metrics", {})
    quality = metrics.get("quality_score")
    quality_text = f"{float(quality):.0f}/100" if quality is not None else "—"

    status = str(run.get("status", "running")).upper()

    if active_stage:
        badge = f"● {LABELS[active_stage]} RUNNING"
        badge_class = "live"
    else:
        badge = f"● {status}"
        badge_class = status.lower()

    st.html(
        f"""
<style>
.dd-live {{
    margin:18px 0;
    padding:18px;
    border:1px solid rgba(148,163,184,.22);
    border-radius:20px;
    background:linear-gradient(145deg,rgba(30,41,59,.55),rgba(15,23,42,.65));
    backdrop-filter:blur(18px) saturate(160%);
    -webkit-backdrop-filter:blur(18px) saturate(160%);
    box-shadow:0 14px 40px rgba(0,0,0,.28), inset 0 1px 0 rgba(255,255,255,.06);
}}

.dd-live-head {{
    display:flex;
    justify-content:space-between;
    align-items:center;
    margin-bottom:16px;
}}

.dd-live-title {{
    color:#f8fafc;
    font-size:16px;
    font-weight:800;
}}

.dd-live-sub {{
    color:#64748b;
    font-size:10px;
    margin-top:3px;
}}

.dd-live-badge {{
    font-size:9px;
    font-weight:850;
    letter-spacing:.08em;
}}

.dd-live-badge.live {{ color:#60a5fa; }}
.dd-live-badge.success {{ color:#4ade80; }}
.dd-live-badge.failed {{ color:#f87171; }}

.dd-flow {{
    display:flex;
    align-items:center;
    gap:5px;
    overflow-x:auto;
}}

.dd-stage {{
    position:relative;
    flex:1 0 118px;
    min-height:105px;
    padding:12px;
    border-radius:14px;
    border:1px solid rgba(148,163,184,.16);
    background:rgba(255,255,255,.045);
    backdrop-filter:blur(10px);
    -webkit-backdrop-filter:blur(10px);
    box-shadow:inset 0 1px 0 rgba(255,255,255,.05);
    overflow:hidden;
    transition:border-color .3s ease, box-shadow .3s ease;
}}

.dd-stage.waiting {{ opacity:.42; }}
.dd-stage.complete {{ border-color:rgba(74,222,128,.4); background:rgba(74,222,128,.05); }}

.dd-stage.active {{
    border-color:rgba(96,165,250,.85);
    background:rgba(96,165,250,.08);
    box-shadow:0 0 28px rgba(59,130,246,.28), inset 0 1px 0 rgba(255,255,255,.08);
    animation:dd-glass-pulse 2.2s ease-in-out infinite;
}}

.dd-stage.active::before {{
    content:"";
    position:absolute;
    inset:0;
    background:linear-gradient(120deg, transparent 30%, rgba(255,255,255,.10) 50%, transparent 70%);
    background-size:220% 100%;
    animation:dd-glass-sheen 2.4s ease-in-out infinite;
    pointer-events:none;
}}

@keyframes dd-glass-pulse {{
    0%, 100% {{ box-shadow:0 0 28px rgba(59,130,246,.28), inset 0 1px 0 rgba(255,255,255,.08); }}
    50%      {{ box-shadow:0 0 40px rgba(59,130,246,.45), inset 0 1px 0 rgba(255,255,255,.12); }}
}}

@keyframes dd-glass-sheen {{
    0%   {{ background-position:130% 0; }}
    100% {{ background-position:-30% 0; }}
}}

.dd-stage.failed {{
    border-color:rgba(248,113,113,.72);
    background:rgba(127,29,29,.22);
}}

.dd-stage-name {{
    color:#cbd5e1;
    font-size:9px;
    font-weight:850;
    letter-spacing:.08em;
}}

.dd-stage-state {{
    color:#64748b;
    font-size:8px;
    font-weight:800;
    margin-top:5px;
}}

.dd-stage.active .dd-stage-state {{ color:#93c5fd; }}
.dd-stage.complete .dd-stage-state {{ color:#86efac; }}
.dd-stage.failed .dd-stage-state {{ color:#fca5a5; }}

.dd-stage-rows {{
    color:#f1f5f9;
    font-size:11px;
    font-weight:800;
    margin-top:27px;
}}

.dd-stage-message {{
    color:#64748b;
    font-size:8px;
    margin-top:4px;
    white-space:nowrap;
    overflow:hidden;
    text-overflow:ellipsis;
}}

.dd-connector {{
    position:relative;
    flex:0 0 28px;
    height:2px;
    background:#334155;
}}

.dd-connector.flowing {{
    background:#3b82f6;
    box-shadow:0 0 8px rgba(59,130,246,.55);
}}

.dd-connector.flowing::after {{
    content:"";
    position:absolute;
    width:6px;
    height:6px;
    top:-2px;
    left:0;
    border-radius:50%;
    background:#60a5fa;
    box-shadow:0 0 8px rgba(96,165,250,.8);
    animation:dd-connector-flow 1s linear infinite;
}}

.dd-connector.failed {{ background:#ef4444; }}

.dd-packets {{
    position:absolute;
    left:12px;
    right:12px;
    top:49px;
    height:8px;
}}

.dd-packets i {{
    position:absolute;
    width:6px;
    height:6px;
    border-radius:50%;
    background:#60a5fa;
    box-shadow:0 0 8px rgba(96,165,250,.8);
    animation:dd-packet-flow 1.15s linear infinite;
}}

.dd-packets i:nth-child(2) {{ animation-delay:.2s; }}
.dd-packets i:nth-child(3) {{ animation-delay:.4s; }}
.dd-packets i:nth-child(4) {{ animation-delay:.6s; }}
.dd-packets i:nth-child(5) {{ animation-delay:.8s; }}
.dd-packets i:nth-child(6) {{ animation-delay:1s; }}
.dd-packets i:nth-child(7) {{ animation-delay:1.2s; }}

.dd-metrics {{
    display:flex;
    gap:7px;
    flex-wrap:wrap;
    margin-top:10px;
}}

.dd-metric {{
    padding:7px 10px;
    border-radius:9px;
    background:rgba(30,41,59,.60);
    color:#64748b;
    font-size:9px;
}}

.dd-metric b {{ color:#e2e8f0; }}

@keyframes dd-packet-flow {{
    0% {{ left:0%; opacity:0; }}
    15% {{ opacity:1; }}
    80% {{ opacity:1; }}
    100% {{ left:95%; opacity:0; }}
}}

@keyframes dd-connector-flow {{
    0% {{ left:0; opacity:0; }}
    15% {{ opacity:1; }}
    85% {{ opacity:1; }}
    100% {{ left:calc(100% - 6px); opacity:0; }}
}}
</style>

<div class="dd-live">
    <div class="dd-live-head">
        <div>
            <div class="dd-live-title">Live Pipeline Execution</div>
            <div class="dd-live-sub">
                {html.escape(str(run.get("dataset", "")))} ·
                Run {html.escape(str(run.get("run_id", "")))}
            </div>
        </div>
        <div class="dd-live-badge {badge_class}">{badge}</div>
    </div>

    <div class="dd-flow">
        {''.join(cards)}
    </div>

    <div class="dd-metrics">
        <div class="dd-metric">Input <b>{int(metrics.get("rows_in") or 0):,}</b></div>
        <div class="dd-metric">Output <b>{int(metrics.get("rows_out") or 0):,}</b></div>
        <div class="dd-metric">Quality <b>{quality_text}</b></div>
        <div class="dd-metric">Repairs <b>{int(metrics.get("repairs") or 0):,}</b></div>
        <div class="dd-metric">Failed checks <b>{int(metrics.get("failed_checks") or 0):,}</b></div>
    </div>
</div>
""",
    )
