"""ui/Settings.py — one-time configuration for Databricks (separate Demo + Enterprise
workspace slots) + all AI providers. Everything persists to database/app_settings.json
via config/settings.py. No code changes are needed anywhere else in the app when these
values change."""
import streamlit as st

from ai import ollama as ollama_client
from config.enterprise import enterprise_status
from config.secrets import mask
from config.settings import (
    FREE_TIER_AI_PROVIDERS,
    is_databricks_configured,
    load_settings,
    save_settings,
)
from dbx_enterprise.connection import test_connection


def _databricks_workspace_form(settings, mode: str, icon: str, blurb: str):
    """Renders one workspace's credential form (Demo or Enterprise) — they're
    independent slots so filling in one never touches the other."""
    db_cfg = settings["databricks"][mode]
    st.markdown(f"#### {icon} {mode.title()} Workspace")
    st.caption(blurb)
    c1, c2 = st.columns(2)
    with c1:
        db_cfg["workspace_url"] = st.text_input(
            "Workspace URL", value=db_cfg["workspace_url"],
            placeholder="https://dbc-xxxxxxxx-xxxx.cloud.databricks.com", key=f"{mode}_url")
        db_cfg["catalog"] = st.text_input("Catalog", value=db_cfg["catalog"], key=f"{mode}_catalog")
        db_cfg["cluster_id"] = st.text_input("Cluster ID (optional)", value=db_cfg["cluster_id"], key=f"{mode}_cluster")
    with c2:
        db_cfg["token"] = st.text_input(
            "Personal Access Token", value=db_cfg["token"], type="password", key=f"{mode}_token",
            help=f"Currently: {mask(db_cfg['token']) or 'not set'}. Generate one under "
                 "User Settings → Developer → Access Tokens in your Databricks workspace.")
        db_cfg["schema"] = st.text_input("Schema", value=db_cfg["schema"], key=f"{mode}_schema")
    db_cfg["http_path"] = st.text_input(
        "HTTP Path (SQL Warehouse or cluster)", value=db_cfg["http_path"], key=f"{mode}_http_path",
        placeholder="/sql/1.0/warehouses/xxxxxxxx",
        help="SQL Warehouses → your warehouse → Connection Details. Required even if you "
             "also filled in Cluster ID — the connector needs the HTTP Path either way.",
    )
    st.write("DEBUG:", db_cfg)
    colA, colB = st.columns([1, 3])
    if colA.button("Test connection", key=f"{mode}_test"):
        if is_databricks_configured(settings, mode):
            with st.spinner(f"Testing {mode.title()} workspace connection..."):
                ok, msg = test_connection(mode=mode)
            (st.success if ok else st.error)(msg)
        else:
            st.warning("Fill in Workspace URL, Token, and HTTP Path first.")


def render():
    st.title("⚙️ Settings")
    st.caption("Enter your keys once. Everything is stored locally and loaded automatically on every run.")

    settings = load_settings()

    mode_choice = st.radio(
        "App mode", ["Demo", "Enterprise"], horizontal=True,
        index=1 if settings.get("mode") == "enterprise" else 0,
        help="Demo Mode: your own free-edition workspace + free-tier AI only. "
             "Enterprise Mode: a paid/production workspace + any configured AI provider.",
    )
    settings["mode"] = mode_choice.lower()

    st.divider()

    # ---------------- Databricks — two independent workspace slots ----------------
    st.subheader("🧱 Databricks")
    st.caption(
        "Demo and Enterprise each have their own workspace credentials — switching the "
        "mode above never mixes them up or requires re-entering keys. Both attempt "
        "Databricks first and silently fall back to local DuckDB if the workspace is "
        "unreachable (cluster asleep, quota, network)."
    )
    _databricks_workspace_form(
        settings, "demo", "🟡",
        "Point this at your own Databricks Free Edition workspace — zero cost, meant for trying the app.",
    )
    st.divider()
    _databricks_workspace_form(
        settings, "enterprise", "🟢",
        "Point this at a paid/production workspace for real customer data.",
    )

    st.divider()

    # ---------------- AI Providers ----------------
    st.subheader("🤖 AI Providers")
    st.caption(
        "Priority order: Ollama → Gemini → OpenRouter → OpenAI → Azure OpenAI → Claude. "
        "The app automatically uses the first one that's configured and reachable. "
        f"**Demo Mode allows genuinely free-tier providers** ({', '.join(p.title() for p in FREE_TIER_AI_PROVIDERS)}) "
        "— OpenAI/Azure OpenAI/Claude have no standing free tier, so they only activate in Enterprise Mode "
        "even if a key is saved below, to avoid a demo silently running up a bill."
    )

    ai_cfg = settings["ai"]

    with st.expander("🦙 Ollama (local, no key required) — free in Demo", expanded=False):
        ai_cfg["ollama"]["url"] = st.text_input("Ollama URL", value=ai_cfg["ollama"]["url"])
        ai_cfg["ollama"]["model"] = st.text_input("Model", value=ai_cfg["ollama"]["model"], key="ollama_model")
        if st.button("Check Ollama reachability"):
            reachable = ollama_client.is_reachable(ai_cfg["ollama"]["url"])
            (st.success if reachable else st.error)("Reachable ✅" if reachable else "Not reachable — is Ollama running?")

    with st.expander("✨ Gemini — free tier available, usable in Demo", expanded=False):
        ai_cfg["gemini"]["api_key"] = st.text_input("API Key", value=ai_cfg["gemini"]["api_key"], type="password", key="gemini_key")
        ai_cfg["gemini"]["model"] = st.text_input("Model", value=ai_cfg["gemini"]["model"], key="gemini_model")

    with st.expander("🔀 OpenRouter — free-tier models available, usable in Demo", expanded=False):
        ai_cfg["openrouter"]["api_key"] = st.text_input("API Key", value=ai_cfg["openrouter"]["api_key"], type="password", key="or_key")
        ai_cfg["openrouter"]["model"] = st.text_input("Model", value=ai_cfg["openrouter"]["model"], key="or_model")

    with st.expander("🟢 OpenAI — Enterprise only (no standing free tier)", expanded=False):
        ai_cfg["openai"]["api_key"] = st.text_input("API Key", value=ai_cfg["openai"]["api_key"], type="password", key="oa_key")
        ai_cfg["openai"]["model"] = st.text_input("Model", value=ai_cfg["openai"]["model"], key="oa_model")

    with st.expander("🔷 Azure OpenAI — Enterprise only (no standing free tier)", expanded=False):
        az = ai_cfg["azure_openai"]
        az["endpoint"] = st.text_input("Endpoint", value=az["endpoint"], key="az_endpoint")
        az["deployment"] = st.text_input("Deployment", value=az["deployment"], key="az_deploy")
        az["api_version"] = st.text_input("API Version", value=az["api_version"], key="az_ver")
        az["api_key"] = st.text_input("Key", value=az["api_key"], type="password", key="az_key")

    with st.expander("🟣 Claude — Enterprise only (no standing free tier)", expanded=False):
        ai_cfg["claude"]["api_key"] = st.text_input("API Key", value=ai_cfg["claude"]["api_key"], type="password", key="claude_key")
        ai_cfg["claude"]["model"] = st.text_input("Model", value=ai_cfg["claude"]["model"], key="claude_model")

    st.divider()

    # ---------------- Pipeline / Quality / Monitoring (native Databricks Job path) ----------------
    with st.expander("🔧 Pipeline, Quality & Monitoring settings", expanded=False):
        st.caption("Table naming and Delta housekeeping apply to the native Databricks Job path "
                   "(Pipeline Studio → ⚡ Run as native Databricks Job). Quality thresholds apply "
                   "to every run, in-app or on Databricks.")
        p = settings["pipeline"]
        c1, c2, c3 = st.columns(3)
        p["bronze_table"] = c1.text_input("Bronze table prefix", value=p["bronze_table"])
        p["silver_table"] = c2.text_input("Silver table prefix", value=p["silver_table"])
        p["gold_table"] = c3.text_input("Gold table prefix", value=p["gold_table"])
        c1, c2, c3 = st.columns(3)
        p["batch_size"] = c1.number_input("Batch size (rows/file)", value=p["batch_size"], step=1000)
        p["max_retries"] = c2.number_input("Job-submit max retries", value=p["max_retries"], min_value=0, max_value=5)
        p["retry_delay_seconds"] = c3.number_input("Retry delay (s)", value=p["retry_delay_seconds"], min_value=1)
        c1, c2 = st.columns(2)
        p["optimize_after_load"] = c1.checkbox("OPTIMIZE after load", value=p["optimize_after_load"])
        p["vacuum_after_load"] = c2.checkbox("VACUUM after load", value=p["vacuum_after_load"])

        q = settings["quality"]
        st.markdown("**Quality thresholds**")
        c1, c2, c3 = st.columns(3)
        q["minimum_score"] = c1.slider("Minimum quality score", 0, 100, value=q["minimum_score"])
        q["duplicate_threshold"] = c2.slider("Duplicate row tolerance", 0.0, 1.0, value=q["duplicate_threshold"])
        q["null_threshold"] = c3.slider("Null-column flag threshold", 0.0, 1.0, value=q["null_threshold"])
        c1, c2 = st.columns(2)
        q["schema_validation"] = c1.checkbox("Schema validation", value=q["schema_validation"])
        q["business_validation"] = c2.checkbox("Business-rule validation", value=q["business_validation"])

        m = settings["monitoring"]
        st.markdown("**Monitoring**")
        c1, c2 = st.columns(2)
        m["history_days"] = c1.number_input("Monitor history window (days)", value=m["history_days"], min_value=1)
        m["audit_logging"] = c2.checkbox("Detailed audit logging", value=m["audit_logging"])

        f = settings["features"]
        st.markdown("**Page visibility**")
        c1, c2, c3, c4 = st.columns(4)
        f["pipeline_builder"] = c1.checkbox("Pipeline Studio", value=f["pipeline_builder"])
        f["business_ai"] = c2.checkbox("Business AI", value=f["business_ai"])
        f["pipeline_monitor"] = c3.checkbox("Monitor", value=f["pipeline_monitor"])
        f["dashboard"] = c4.checkbox("Dashboard", value=f["dashboard"])

    st.divider()
    if st.button("💾 Save Settings", type="primary", use_container_width=True):
        save_settings(settings)
        st.success("Settings saved.")
        st.rerun()

    st.divider()
    st.subheader("Enterprise readiness")
    status = enterprise_status()
    if status["ready"]:
        st.success(f"{status['mode'].title()} workspace configured — `{status['workspace_url']}`, "
                   f"catalog `{status['catalog']}.{status['schema']}`.")
    else:
        st.info(f"No Databricks configured for {status['mode'].title()} Mode yet — running on local DuckDB storage.")
    with st.expander("What Enterprise Mode additionally unlocks"):
        for k, v in status["features"].items():
            st.write(f"- **{k.replace('_', ' ').title()}**: {v}")
