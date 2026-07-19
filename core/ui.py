"""
Shared enterprise UI styles and reusable helper components.

This file preserves the existing public functions used by all pages:

    inject_global_css()
    status_badge()
    sidebar_brand()

Existing pages will continue working without requiring changes.
"""

import html

import streamlit as st


# =========================================================
# PIPELINE STATUS COLORS
# =========================================================

STATUS_COLORS = {
    "SUCCEEDED": "#22C55E",
    "REPAIRED": "#3B82F6",
    "FAILED": "#EF4444",
    "RUNNING": "#6366F1",
    "PENDING": "#94A3B8",
    "SKIPPED": "#F59E0B",
    "PARTIALLY_REPAIRED": "#F59E0B",
}


# =========================================================
# GLOBAL ENTERPRISE UI
# =========================================================

def inject_global_css():
    """
    Apply the shared DataDoctor AI enterprise visual theme.

    This changes styling only. It does not change page navigation,
    pipeline execution, buttons, forms, tables, AI logic, or data.
    """

    st.markdown(
        """
        <style>

        /* =====================================================
           APPLICATION
        ===================================================== */

        .stApp {
            background:
                radial-gradient(
                    circle at 75% 0%,
                    rgba(79, 70, 229, 0.08),
                    transparent 30%
                ),
                radial-gradient(
                    circle at 20% 15%,
                    rgba(14, 165, 233, 0.05),
                    transparent 25%
                ),
                #0B0F17;
        }

        .block-container {
            max-width: 1380px;
            padding-top: 2rem;
            padding-bottom: 3rem;
            padding-left: 2.2rem;
            padding-right: 2.2rem;
        }


        /* =====================================================
           SIDEBAR
        ===================================================== */

        section[data-testid="stSidebar"] {
            background:
                linear-gradient(
                    180deg,
                    #111827 0%,
                    #0D131F 100%
                );

            border-right:
                1px solid
                rgba(148, 163, 184, 0.14);
        }

        section[data-testid="stSidebar"]
        > div {
            padding-top: 0.8rem;
        }

        section[data-testid="stSidebar"]
        [data-testid="stMarkdownContainer"] p {
            line-height: 1.5;
        }


        /* =====================================================
           DATADOCTOR BRAND
        ===================================================== */

        .ddai-brand {
            position: relative;

            overflow: hidden;

            margin:
                0.25rem
                0
                1rem
                0;

            padding:
                1.15rem
                1rem;

            border:
                1px solid
                rgba(99, 102, 241, 0.28);

            border-radius:
                16px;

            background:
                linear-gradient(
                    145deg,
                    rgba(30, 41, 59, 0.96),
                    rgba(15, 23, 42, 0.96)
                );

            box-shadow:
                0 14px 35px
                rgba(0, 0, 0, 0.22);
        }

        .ddai-brand::before {
            content: "";

            position: absolute;

            width: 130px;
            height: 130px;

            top: -80px;
            right: -55px;

            border-radius: 50%;

            background:
                rgba(99, 102, 241, 0.18);

            filter:
                blur(8px);
        }

        .ddai-brand-row {
            position: relative;

            display: flex;

            align-items: center;

            gap: 0.8rem;
        }

        .ddai-logo {
            display: flex;

            align-items: center;
            justify-content: center;

            min-width: 44px;
            width: 44px;
            height: 44px;

            border-radius: 13px;

            font-size: 1.35rem;

            background:
                linear-gradient(
                    135deg,
                    #6366F1,
                    #0EA5E9
                );

            box-shadow:
                0 8px 22px
                rgba(79, 70, 229, 0.32);
        }

        .ddai-brand-name {
            margin: 0;

            color: #F8FAFC;

            font-size: 1.08rem;

            font-weight: 750;

            letter-spacing:
                -0.02em;
        }

        .ddai-brand-edition {
            display: inline-block;

            margin-top: 0.22rem;

            padding:
                0.1rem
                0.45rem;

            border:
                1px solid
                rgba(56, 189, 248, 0.25);

            border-radius:
                999px;

            color: #7DD3FC;

            background:
                rgba(14, 165, 233, 0.08);

            font-size: 0.64rem;

            font-weight: 700;

            letter-spacing:
                0.08em;

            text-transform:
                uppercase;
        }

        .ddai-brand-description {
            position: relative;

            margin-top: 0.9rem;

            color: #94A3B8;

            font-size: 0.78rem;

            line-height: 1.55;
        }

        .ddai-brand-status {
            position: relative;

            display: flex;

            align-items: center;

            gap: 0.45rem;

            margin-top: 0.85rem;

            color: #CBD5E1;

            font-size: 0.72rem;
        }

        .ddai-live-dot {
            width: 7px;
            height: 7px;

            border-radius: 50%;

            background: #22C55E;

            box-shadow:
                0 0 0 4px
                rgba(34, 197, 94, 0.11);
        }


        /* =====================================================
           PAGE HEADINGS
        ===================================================== */

        h1 {
            color: #F8FAFC;

            font-weight: 760;

            letter-spacing:
                -0.035em;
        }

        h2 {
            color: #F1F5F9;

            font-weight: 720;

            letter-spacing:
                -0.025em;
        }

        h3 {
            color: #E2E8F0;

            font-weight: 680;

            letter-spacing:
                -0.015em;
        }

        p {
            color: #CBD5E1;
        }


        /* =====================================================
           METRIC CARDS
        ===================================================== */

        div[data-testid="stMetric"] {
            min-height: 125px;

            padding:
                1.15rem
                1.2rem;

            border:
                1px solid
                rgba(148, 163, 184, 0.14);

            border-radius:
                16px;

            background:
                linear-gradient(
                    145deg,
                    rgba(24, 32, 47, 0.96),
                    rgba(15, 23, 42, 0.96)
                );

            box-shadow:
                0 12px 30px
                rgba(0, 0, 0, 0.17);

            transition:
                transform 0.18s ease,
                border-color 0.18s ease,
                box-shadow 0.18s ease;
        }

        div[data-testid="stMetric"]:hover {
            transform:
                translateY(-2px);

            border-color:
                rgba(99, 102, 241, 0.35);

            box-shadow:
                0 16px 38px
                rgba(0, 0, 0, 0.22);
        }

        div[data-testid="stMetricLabel"] {
            color: #94A3B8;

            font-size: 0.8rem;

            font-weight: 650;

            letter-spacing:
                0.025em;
        }

        div[data-testid="stMetricValue"] {
            color: #F8FAFC;

            font-weight: 760;

            letter-spacing:
                -0.035em;
        }


        /* =====================================================
           DATADOCTOR CARDS
        ===================================================== */

        .ddai-card {
            position: relative;

            overflow: hidden;

            margin-bottom: 1rem;

            padding:
                1.25rem
                1.35rem;

            border:
                1px solid
                rgba(148, 163, 184, 0.15);

            border-radius:
                16px;

            background:
                linear-gradient(
                    145deg,
                    rgba(24, 32, 47, 0.96),
                    rgba(15, 23, 42, 0.96)
                );

            box-shadow:
                0 12px 30px
                rgba(0, 0, 0, 0.16);
        }

        .ddai-card::before {
            content: "";

            position: absolute;

            width: 3px;

            top: 18px;
            bottom: 18px;
            left: 0;

            border-radius:
                0
                4px
                4px
                0;

            background:
                linear-gradient(
                    180deg,
                    #6366F1,
                    #0EA5E9
                );
        }

        .ddai-title {
            margin-bottom: 0.25rem;

            color: #F1F5F9;

            font-size: 1.05rem;

            font-weight: 700;
        }

        .ddai-subtle {
            color: #94A3B8;

            font-size: 0.84rem;
        }


        /* =====================================================
           STATUS BADGES
        ===================================================== */

        .ddai-badge {
            display: inline-flex;

            align-items: center;

            justify-content: center;

            padding:
                0.22rem
                0.65rem;

            border:
                1px solid
                rgba(255, 255, 255, 0.12);

            border-radius:
                999px;

            color: #FFFFFF;

            font-size: 0.7rem;

            font-weight: 750;

            letter-spacing:
                0.035em;

            line-height: 1.2;

            box-shadow:
                inset 0 1px 0
                rgba(255, 255, 255, 0.08);
        }


        /* =====================================================
           BUTTONS
        ===================================================== */

        .stButton > button,
        .stFormSubmitButton > button {
            min-height: 2.65rem;

            border:
                1px solid
                rgba(99, 102, 241, 0.35);

            border-radius:
                10px;

            color: #E2E8F0;

            background:
                linear-gradient(
                    145deg,
                    rgba(30, 41, 59, 0.96),
                    rgba(17, 24, 39, 0.96)
                );

            font-weight: 650;

            transition:
                transform 0.16s ease,
                border-color 0.16s ease,
                box-shadow 0.16s ease;
        }

        .stButton > button:hover,
        .stFormSubmitButton > button:hover {
            transform:
                translateY(-1px);

            border-color:
                rgba(99, 102, 241, 0.8);

            color: #FFFFFF;

            box-shadow:
                0 8px 20px
                rgba(79, 70, 229, 0.18);
        }

        .stButton > button[kind="primary"],
        .stFormSubmitButton > button[kind="primary"] {
            border:
                1px solid
                rgba(129, 140, 248, 0.55);

            color: #FFFFFF;

            background:
                linear-gradient(
                    135deg,
                    #4F46E5,
                    #2563EB
                );

            box-shadow:
                0 9px 22px
                rgba(79, 70, 229, 0.22);
        }


        /* =====================================================
           INPUTS
        ===================================================== */

        div[data-baseweb="input"] > div,
        div[data-baseweb="textarea"] > div,
        div[data-baseweb="select"] > div {
            border-color:
                rgba(148, 163, 184, 0.2);

            border-radius:
                10px;

            background:
                rgba(15, 23, 42, 0.82);
        }

        div[data-baseweb="input"] > div:focus-within,
        div[data-baseweb="textarea"] > div:focus-within,
        div[data-baseweb="select"] > div:focus-within {
            border-color:
                rgba(99, 102, 241, 0.85);

            box-shadow:
                0 0 0 1px
                rgba(99, 102, 241, 0.3);
        }


        /* =====================================================
           DATAFRAMES AND TABLES
        ===================================================== */

        div[data-testid="stDataFrame"] {
            overflow: hidden;

            border:
                1px solid
                rgba(148, 163, 184, 0.16);

            border-radius:
                14px;

            background:
                rgba(15, 23, 42, 0.78);

            box-shadow:
                0 10px 25px
                rgba(0, 0, 0, 0.13);
        }


        /* =====================================================
           CONTAINERS
        ===================================================== */

        div[data-testid="stVerticalBlockBorderWrapper"] {
            border-color:
                rgba(148, 163, 184, 0.16);

            border-radius:
                16px;

            background:
                linear-gradient(
                    145deg,
                    rgba(24, 32, 47, 0.78),
                    rgba(15, 23, 42, 0.72)
                );

            box-shadow:
                0 10px 28px
                rgba(0, 0, 0, 0.13);
        }


        /* =====================================================
           ALERTS
        ===================================================== */

        div[data-testid="stAlert"] {
            border-radius:
                12px;
        }


        /* =====================================================
           TABS
        ===================================================== */

        button[data-baseweb="tab"] {
            font-weight: 650;
        }


        /* =====================================================
           EXPANDERS
        ===================================================== */

        details {
            border:
                1px solid
                rgba(148, 163, 184, 0.15);

            border-radius:
                12px;

            background:
                rgba(15, 23, 42, 0.65);
        }


        /* =====================================================
           CODE
        ===================================================== */

        code {
            font-size: 0.85rem;
        }


        /* =====================================================
           SCROLLBAR
        ===================================================== */

        ::-webkit-scrollbar {
            width: 9px;
            height: 9px;
        }

        ::-webkit-scrollbar-track {
            background: #0B0F17;
        }

        ::-webkit-scrollbar-thumb {
            border:
                2px solid
                #0B0F17;

            border-radius:
                999px;

            background: #334155;
        }

        ::-webkit-scrollbar-thumb:hover {
            background: #475569;
        }


        /* =====================================================
           SMALL SCREEN SUPPORT
        ===================================================== */

        @media (
            max-width: 900px
        ) {

            .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
            }

            .ddai-brand-description {
                display: none;
            }

        }

        </style>
        """,
        unsafe_allow_html=True,
    )


# =========================================================
# STATUS BADGE
# =========================================================

def status_badge(status: str) -> str:
    """
    Return a safe colored HTML badge for a pipeline or step status.
    """

    normalized_status = str(
        status or "UNKNOWN"
    ).strip().upper()

    color = STATUS_COLORS.get(
        normalized_status,
        "#64748B",
    )

    safe_status = html.escape(
        normalized_status
    )

    return (
        '<span class="ddai-badge" '
        f'style="background:{color};">'
        f"{safe_status}"
        "</span>"
    )


# =========================================================
# SIDEBAR BRAND
# =========================================================

def execution_backend_meta(backend: str) -> dict:
    """
    Map an internal execution_backend value to a truthful, user-facing
    label/color/description. This is the single source of truth used by
    every page so 'Simulation' vs 'Databricks' is never ambiguous.
    """
    normalized = str(backend or "").strip()
    table = {
        "databricks-serverless-pyspark": {
            "label": "Databricks (real PySpark)",
            "color": "#22C55E",
            "icon": "🟢",
            "note": "Executed on a real Databricks serverless job — Delta tables were written in your workspace.",
        },
        "verified-local-data": {
            "label": "Verified — your uploaded data",
            "color": "#3B82F6",
            "icon": "🔵",
            "note": "Ran locally against the file you uploaded (pandas). Real rows, real profiling — not a Spark cluster.",
        },
        "local-orchestration-simulation": {
            "label": "Simulation",
            "color": "#F59E0B",
            "icon": "🟠",
            "note": "No uploaded file / no Databricks configured — orchestration, dependencies, and retries are simulated for demo purposes.",
        },
    }
    return table.get(
        normalized,
        {
            "label": normalized or "Unknown",
            "color": "#64748B",
            "icon": "⚪",
            "note": "Execution backend not recorded for this run.",
        },
    )


def execution_backend_badge(backend: str) -> str:
    """
    Return a safe colored HTML badge that honestly states whether a run was
    Simulation, Verified-local, or real Databricks — no page should claim
    'PySpark Engine' without this label next to it.
    """
    meta = execution_backend_meta(backend)
    safe_label = html.escape(meta["label"])
    return (
        '<span class="ddai-badge" '
        f'style="background:{meta["color"]};" '
        f'title="{html.escape(meta["note"])}">'
        f'{meta["icon"]} {safe_label}'
        "</span>"
    )


def render_execution_backend(backend: str):
    """Streamlit-render the execution backend badge plus its explanation."""
    meta = execution_backend_meta(backend)
    st.markdown(execution_backend_badge(backend), unsafe_allow_html=True)
    st.caption(meta["note"])


def ai_provider_status() -> dict:
    """
    Wrap core.llm_provider.llm_status() so every page shows the SAME
    provider truth (Claude API / Ollama / Internal rule-based engine).
    Never fabricates a provider (e.g. Gemini) that isn't actually wired up.
    """
    try:
        from core.llm_provider import llm_status
        status = llm_status()
    except Exception as exc:
        status = {
            "tier": 3,
            "provider": "Internal rule-based engine",
            "model": None,
            "available": True,
            "note": f"Provider check failed ({exc}); using deterministic fallback.",
        }
    tier_colors = {1: "#7C3AED", 2: "#0EA5E9", 3: "#64748B"}
    tier_icons = {1: "✨", 2: "🖥️", 3: "🛠️"}
    status["color"] = tier_colors.get(status.get("tier"), "#64748B")
    status["icon"] = tier_icons.get(status.get("tier"), "🛠️")
    # "Internal rule-based engine" is what tier 3 actually is — rename for clarity.
    if status.get("tier") == 3:
        status["provider"] = "Internal rule-based engine"
    return status


def render_ai_provider_badge(inline: bool = True):
    """
    Render the 'which engine answered' indicator. Call this near the top of
    any AI-powered page (Chat, AI Agent, AI Code Assistant) or in the
    sidebar so it's always visible, never buried in Settings alone.
    """
    status = ai_provider_status()
    model_suffix = f" · {status['model']}" if status.get("model") else ""
    badge_html = (
        '<span class="ddai-badge" '
        f'style="background:{status["color"]};" '
        f'title="{html.escape(status.get("note", ""))}">'
        f'{status["icon"]} {html.escape(status["provider"])}{html.escape(model_suffix)}'
        "</span>"
    )
    if inline:
        st.markdown(f"**AI Provider:** {badge_html}", unsafe_allow_html=True)
    else:
        st.markdown(badge_html, unsafe_allow_html=True)


def render_rag_transparency(
    source_names=None,
    dataset_used: bool = False,
    logs_used: bool = False,
    retrieved: bool = False,
):
    """
    Make it obvious whether an answer came from uploaded files, project
    documents, logs, or just the general LLM with no retrieval at all.
    Pass whatever the calling page already knows — this only renders it
    consistently.
    """
    source_names = source_names or []
    doc_sources = [s for s in source_names if s != "DataDoctor incident memory"]
    incident_used = "DataDoctor incident memory" in source_names

    chips = []
    if dataset_used:
        chips.append(("📊", "Uploaded dataset", "#3B82F6"))
    if doc_sources:
        chips.append(("📄", f"Project docs ({len(doc_sources)})", "#22C55E"))
    if incident_used:
        chips.append(("🧾", "Past incident memory", "#F59E0B"))
    if logs_used:
        chips.append(("🗒️", "Pipeline logs", "#EAB308"))

    if not chips and not retrieved:
        chips.append(("🧠", "General LLM knowledge — no retrieval used", "#64748B"))

    badges = " ".join(
        '<span class="ddai-badge" style="background:{color};">{icon} {label}</span>'.format(
            color=color, icon=icon, label=html.escape(label)
        )
        for icon, label, color in chips
    )
    st.markdown(f"**Grounded on:** {badges}", unsafe_allow_html=True)
    if doc_sources:
        st.caption("Sources: " + ", ".join(html.escape(s) for s in doc_sources))


def sidebar_brand():
    """
    Display enterprise DataDoctor AI branding.

    Existing sidebar pages and Streamlit navigation are not changed.
    """

    brand_html = """
<div class="ddai-brand">
<div class="ddai-brand-row">
<div class="ddai-logo">🩺</div>
<div>
<div class="ddai-brand-name">DataDoctor AI</div>
<span class="ddai-brand-edition">DataOps Intelligence</span>
</div>
</div>
<div class="ddai-brand-description">
AI-assisted lakehouse observability, pipeline diagnostics and recovery.
</div>
<div class="ddai-brand-status">
<span class="ddai-live-dot"></span>
Operations console available
</div>
</div>
"""

    st.sidebar.markdown(
        brand_html,
        unsafe_allow_html=True,
    )

    try:
        status = ai_provider_status()
        model_suffix = f" · {status['model']}" if status.get("model") else ""
        st.sidebar.markdown(
            '<div style="margin:-0.4rem 0 0.8rem 0;">'
            '<span class="ddai-badge" '
            f'style="background:{status["color"]};" '
            f'title="{html.escape(status.get("note", ""))}">'
            f'{status["icon"]} {html.escape(status["provider"])}{html.escape(model_suffix)}'
            "</span></div>",
            unsafe_allow_html=True,
        )
    except Exception:
        pass

    st.sidebar.divider()