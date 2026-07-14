"""Shared CSS + small UI helper components used across all pages."""

import streamlit as st


STATUS_COLORS = {
    "SUCCEEDED": "#3FB950",
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

        /* Main application layout */
        .block-container {
            padding-top: 2rem;
            max-width: 1200px;
        }


        /* Status badge */
        .ddai-badge {
            display: inline-block;

            padding: 2px 10px;

            border-radius: 12px;

            font-size: 0.75rem;
            font-weight: 600;

            letter-spacing: 0.02em;

            color: white;
        }


        /* Shared application cards */
        .ddai-card {
            padding: 1.1rem 1.3rem;

            margin-bottom: 0.8rem;

            border:
                1px solid
                #30363D;

            border-radius: 10px;

            background:
                #161B22;
        }


        .ddai-title {
            margin-bottom: 0.2rem;

            font-size: 1.05rem;

            font-weight: 600;
        }


        .ddai-subtle {
            color: #8B949E;

            font-size: 0.85rem;
        }


        code {
            font-size: 0.85rem;
        }


        /* ───────────────────────────── */
        /* DataDoctor AI sidebar brand  */
        /* ───────────────────────────── */

        .dd-sidebar-brand {

            margin-top: 22px;

            margin-bottom: 8px;

            padding:
                20px
                16px;

            border:
                1px solid
                rgba(
                    132,
                    112,
                    255,
                    0.28
                );

            border-radius:
                16px;

            background:

                radial-gradient(
                    circle
                    at 5% 5%,

                    rgba(
                        102,
                        78,
                        255,
                        0.22
                    ),

                    transparent
                    52%
                ),

                linear-gradient(
                    145deg,

                    rgba(
                        39,
                        39,
                        61,
                        0.92
                    ),

                    rgba(
                        20,
                        24,
                        34,
                        0.96
                    )
                );

            box-shadow:

                0
                12px
                34px

                rgba(
                    0,
                    0,
                    0,
                    0.25
                );
        }


        .dd-brand-header {

            display:
                flex;

            align-items:
                center;

            gap:
                12px;
        }


        .dd-brand-icon {

            display:
                flex;

            align-items:
                center;

            justify-content:
                center;

            width:
                45px;

            height:
                45px;

            flex-shrink:
                0;

            border-radius:
                13px;

            background:

                linear-gradient(
                    135deg,

                    #715AFF,

                    #39BFFF
                );

            box-shadow:

                0
                8px
                22px

                rgba(
                    100,
                    79,
                    255,
                    0.38
                );

            font-size:
                24px;
        }


        .dd-brand-name {

            color:
                #F7F8FF;

            font-size:
                20px;

            font-weight:
                800;

            line-height:
                1.05;

            letter-spacing:
                -0.45px;
        }


        .dd-brand-ai {

            color:
                #9A8EFF;
        }


        .dd-brand-badge {

            display:
                inline-block;

            margin-top:
                7px;

            padding:
                3px
                7px;

            border-radius:
                5px;

            color:
                #B0A7FF;

            background:

                rgba(
                    121,
                    100,
                    255,
                    0.14
                );

            font-size:
                8px;

            font-weight:
                750;

            letter-spacing:
                1.05px;
        }


        .dd-brand-description {

            margin-top:
                16px;

            color:
                #A8AFBD;

            font-size:
                12.5px;

            line-height:
                1.55;
        }


        .dd-brand-status {

            display:
                flex;

            align-items:
                center;

            gap:
                7px;

            margin-top:
                14px;

            color:
                #7BD8AA;

            font-size:
                10.5px;

            font-weight:
                600;
        }


        .dd-status-dot {

            width:
                7px;

            height:
                7px;

            border-radius:
                50%;

            background:
                #52DB99;

            box-shadow:

                0
                0
                9px

                rgba(
                    82,
                    219,
                    153,
                    0.85
                );
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


def status_badge(status: str) -> str:

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

    st.sidebar.markdown(
        """
        <div class="dd-sidebar-brand">

            <div class="dd-brand-header">

                <div class="dd-brand-icon">
                    🩺
                </div>

                <div>

                    <div class="dd-brand-name">

                        DataDoctor

                        <span class="dd-brand-ai">
                            AI
                        </span>

                    </div>


                    <div class="dd-brand-badge">

                        AGENTIC DATA OPS

                    </div>

                </div>

            </div>


            <div class="dd-brand-description">

                Intelligent Lakehouse

                <br>

                Pipeline Operations

            </div>


            <div class="dd-brand-status">

                <span class="dd-status-dot">
                </span>

                System operational

            </div>

        </div>
        """,

        unsafe_allow_html=True,
    )


    st.sidebar.divider()