"""
app.py — DataDoctorAI entry point.

Sets up global page config, shared session state, and the sidebar navigation across
all pages (Dashboard, Pipeline Studio, Business AI, Monitor, Replay, Settings, About).

Run with:  streamlit run app.py
"""
import streamlit as st
import textwrap

from config.demo import DEMO_MODE_NOTICE
from config.settings import current_mode, get_feature_flags, load_settings
from storage.router import describe_configuration
from ui import About, BusinessAI, Dashboard, Monitor, PipelineStudio, Replay, Settings

st.set_page_config(
    page_title="DataDoctorAI",
    page_icon="🩺",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---- Shared session state ----
if "active_dataset" not in st.session_state:
    st.session_state.active_dataset = None
if "last_run_id" not in st.session_state:
    st.session_state.last_run_id = None
if "bronze_df" not in st.session_state:
    st.session_state.bronze_df = None
if "silver_result" not in st.session_state:
    st.session_state.silver_result = None
if "gold_result" not in st.session_state:
    st.session_state.gold_result = None

settings = load_settings()
mode = current_mode(settings)
features = get_feature_flags(settings)

all_pages = [
    ("dashboard", st.Page(Dashboard.render, title="Dashboard", icon="📊", url_path="dashboard")),
    ("pipeline_builder", st.Page(PipelineStudio.render, title="Pipeline Studio", icon="🧪", url_path="pipeline-studio")),
    ("business_ai", st.Page(BusinessAI.render, title="Business AI", icon="🤖", url_path="business-ai")),
    ("pipeline_monitor", st.Page(Monitor.render, title="Monitor", icon="🛰️", url_path="monitor")),
    (None, st.Page(Replay.render, title="Replay", icon="⏪", url_path="replay")),
    (None, st.Page(Settings.render, title="Settings", icon="⚙️", url_path="settings")),
    (None, st.Page(About.render, title="About", icon="ℹ️", url_path="about")),
]
# feature flag of None = always shown (Replay/Settings/About aren't gate-able — you
# always need Settings to turn features back on)
pages = [page for flag, page in all_pages if flag is None or features.get(flag, True)]

with st.sidebar:
    st.markdown("## 🩺 DataDoctorAI")

    mode_label = "ENTERPRISE" if mode == "enterprise" else "DEMO"
    mode_class = "enterprise" if mode == "enterprise" else "demo"

    cfg = describe_configuration(mode)
    dbx_ready = cfg["databricks_configured"]

    if dbx_ready:
        backend_label = "DATABRICKS"
        backend_detail = "Workspace connected"
        backend_class = "connected"
    else:
        backend_label = "DUCKDB"
        backend_detail = "Local fallback"
        backend_class = "local"

    st.html(
        textwrap.dedent(
            f"""
            <style>
        .dd-sidebar-card {{
            position: relative;
            overflow: hidden;
            margin: 8px 0 18px 0;
            padding: 16px;
            border-radius: 18px;
            border: 1px solid rgba(148,163,184,.18);
            background:
                radial-gradient(
                    circle at 90% 0%,
                    rgba(96,165,250,.14),
                    transparent 42%
                ),
                linear-gradient(
                    145deg,
                    rgba(15,23,42,.92),
                    rgba(30,41,59,.72)
                );
            box-shadow:
                0 12px 30px rgba(0,0,0,.20),
                inset 0 1px 0 rgba(255,255,255,.04);
        }}

        .dd-sidebar-top {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            margin-bottom: 13px;
        }}

        .dd-sidebar-mode {{
            display: inline-flex;
            align-items: center;
            gap: 7px;
            font-size: 11px;
            font-weight: 800;
            letter-spacing: .09em;
            color: #e2e8f0;
        }}

        .dd-sidebar-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            display: inline-block;
            background: #fbbf24;
            box-shadow: 0 0 10px rgba(251,191,36,.55);
            animation: dd-sidebar-pulse 1.8s ease-in-out infinite;
        }}

        .dd-sidebar-mode.enterprise .dd-sidebar-dot {{
            background: #4ade80;
            box-shadow: 0 0 10px rgba(74,222,128,.55);
        }}

        .dd-sidebar-live {{
            font-size: 9px;
            font-weight: 800;
            letter-spacing: .08em;
            color: #64748b;
        }}

        .dd-sidebar-backend {{
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 11px;
            border-radius: 12px;
            background: rgba(15,23,42,.52);
            border: 1px solid rgba(148,163,184,.10);
        }}

        .dd-sidebar-backend-icon {{
            width: 30px;
            height: 30px;
            display: grid;
            place-items: center;
            border-radius: 9px;
            background: rgba(96,165,250,.10);
            font-size: 14px;
        }}

        .dd-sidebar-backend-name {{
            color: #e2e8f0;
            font-size: 11px;
            font-weight: 800;
            letter-spacing: .05em;
        }}

        .dd-sidebar-backend-detail {{
            color: #64748b;
            font-size: 9px;
            margin-top: 2px;
        }}

        .dd-sidebar-status {{
            margin-left: auto;
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: #4ade80;
            box-shadow: 0 0 8px rgba(74,222,128,.55);
        }}

        .dd-sidebar-status.local {{
            background: #60a5fa;
            box-shadow: 0 0 8px rgba(96,165,250,.55);
        }}

        .dd-sidebar-flow {{
            display: flex;
            align-items: center;
            gap: 5px;
            margin-top: 12px;
        }}

        .dd-sidebar-flow span {{
            height: 3px;
            flex: 1;
            border-radius: 10px;
            background: rgba(96,165,250,.18);
            overflow: hidden;
            position: relative;
        }}

        .dd-sidebar-flow span::after {{
            content: "";
            position: absolute;
            width: 35%;
            height: 100%;
            left: -40%;
            background: linear-gradient(
                90deg,
                transparent,
                rgba(96,165,250,.9),
                transparent
            );
            animation: dd-sidebar-flow 2.2s linear infinite;
        }}

        .dd-sidebar-flow span:nth-child(2)::after {{
            animation-delay: .45s;
        }}

        .dd-sidebar-flow span:nth-child(3)::after {{
            animation-delay: .9s;
        }}

        @keyframes dd-sidebar-pulse {{
            0%, 100% {{ transform: scale(.8); opacity: .65; }}
            50% {{ transform: scale(1.2); opacity: 1; }}
        }}

        @keyframes dd-sidebar-flow {{
            0% {{ left: -40%; }}
            100% {{ left: 110%; }}
        }}
        </style>

        <div class="dd-sidebar-card">
            <div class="dd-sidebar-top">
                <div class="dd-sidebar-mode {mode_class}">
                    <span class="dd-sidebar-dot"></span>
                    {mode_label} MODE
                </div>
                <div class="dd-sidebar-live">RUNTIME</div>
            </div>

            <div class="dd-sidebar-backend">
                <div class="dd-sidebar-backend-icon">
                    {"DB" if dbx_ready else "DB"}
                </div>
                <div>
                    <div class="dd-sidebar-backend-name">
                        {backend_label}
                    </div>
                    <div class="dd-sidebar-backend-detail">
                        {backend_detail}
                    </div>
                </div>
                <span class="dd-sidebar-status {backend_class}"></span>
            </div>

            <div class="dd-sidebar-flow">
                <span></span>
                <span></span>
                <span></span>
            </div>
            </div>
            """
        ).strip(),
    )

    st.divider()


nav = st.navigation(pages)
nav.run()
