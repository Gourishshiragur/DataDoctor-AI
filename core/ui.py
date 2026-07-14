"""Shared CSS + small UI helper components used across all pages."""

import streamlit as st


STATUS_COLORS = {
    "SUCCEEDED": "#3FB950",
    "REPAIRED": "#2F81F7",
    "FAILED": "#F85149",
    "RUNNING": "#5B8DEF",
    "PENDING": "#8B949E",
    "SKIPPED": "#D29922",
    "PARTIALLY_REPAIRED": "#D29922",
}


def inject_global_css():
    st.markdown(
        """
        <style>

        .block-container {
            padding-top: 2rem;
            max-width: 1200px;
        }

        .ddai-badge {
            display: inline-block;
            padding: 2px 10px;
            border-radius: 12px;
            font-size: 0.75rem;
            font-weight: 600;
            letter-spacing: 0.02em;
            color: white;
        }

        .ddai-card {
            background: #161B22;
            border: 1px solid #30363D;
            border-radius: 10px;
            padding: 1.1rem 1.3rem;
            margin-bottom: 0.8rem;
        }

        .ddai-title {
            font-size: 1.05rem;
            font-weight: 600;
            margin-bottom: 0.2rem;
        }

        .ddai-subtle {
            color: #8B949E;
            font-size: 0.85rem;
        }

        code {
            font-size: 0.85rem;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


def status_badge(status: str) -> str:
    """
    Return a colored HTML badge for a pipeline or step status.
    """

    color = STATUS_COLORS.get(
        status,
        "#8B949E",
    )

    return (
        f'<span class="ddai-badge" '
        f'style="background:{color}">'
        f'{status}'
        f'</span>'
    )


def sidebar_brand():
    """
    Display DataDoctor AI branding in the sidebar.
    """

    st.sidebar.markdown(
        "### 🩺 DataDoctor AI\n"
        "<span class='ddai-subtle'>"
        "Lakehouse pipeline ops console"
        "</span>",
        unsafe_allow_html=True,
    )

    st.sidebar.divider()