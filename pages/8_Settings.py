"""
DataDoctor AI — Settings & Configuration

Configure which LLM powers the AI features, view RAG memory status,
and manage the knowledge base.
"""
import streamlit as st

from core.llm_provider import (
    DEFAULT_OLLAMA_MODEL,
    DEFAULT_GROQ_MODEL,
    _claude_api_key,
    _claude_model,
    _groq_api_key,
    _groq_base_url,
    _groq_model,
    _ollama_available,
    _ollama_url,
    _provider_mode,
    llm_status,
    set_session_api_key,
    clear_session_api_key,
    session_api_key_active,
    set_session_provider_mode,
)
from core.rag_memory import (
    clear_knowledge,
    incident_count,
    knowledge_count,
    memory_stats,
)
from core.ui import inject_global_css, sidebar_brand

inject_global_css()
sidebar_brand()

st.title("⚙️ Settings")
st.caption("Configure AI providers, view RAG memory status, and manage the knowledge base.")

# ── LLM Provider status ────────────────────────────────────────────────────
st.subheader("AI Provider")

status = llm_status()

col1, col2 = st.columns([2, 1])
with col1:
    t1, t2, t3, t4 = st.tabs([
        "Tier 1 — Claude API",
        "Tier 2 — Groq (free hosted)",
        "Tier 3 — Ollama (local)",
        "Tier 4 — Fallback",
    ])

    with t1:
        st.markdown("**Claude API (Anthropic)** — best quality, works on Streamlit Cloud")
        if _claude_api_key():
            byok_label = " (your key, this session only)" if session_api_key_active() else ""
            st.success(f"✅ Active{byok_label} — model: `{_claude_model()}`")
        else:
            st.warning("Not configured")

        st.divider()
        st.markdown("**Bring your own key (BYOK)**")
        st.caption(
            "Paste your own Anthropic key to use Tier 1 for just your session. "
            "It is kept only in this browser session's memory — never written to disk, "
            "never saved to the app's data files, and never shared with other users. "
            "It's cleared automatically when your session ends, or you can clear it now."
        )
        if session_api_key_active():
            byok_col1, byok_col2 = st.columns([3, 1])
            with byok_col1:
                st.success("Your session key is active.")
            with byok_col2:
                if st.button("Clear my key", type="secondary"):
                    clear_session_api_key()
                    st.rerun()
        else:
            with st.form("byok_form", clear_on_submit=True):
                byok_input = st.text_input(
                    "Your Anthropic API key",
                    type="password",
                    placeholder="sk-ant-...",
                )
                byok_submit = st.form_submit_button("Use my key for this session")
                if byok_submit and byok_input.strip():
                    set_session_api_key(byok_input.strip())
                    st.rerun()

        st.divider()
        st.markdown("**Or a deployment-wide key (for the app owner)**")
        st.markdown(
            "To enable Tier 1 for everyone using this deployment:\n"
            "1. Go to your app on **share.streamlit.io**\n"
            "2. Click **⋮ → Settings → Secrets**\n"
            "3. Add:\n"
            "```toml\nANTHROPIC_API_KEY = \"sk-ant-...\"\n```\n"
            "4. Save — the app restarts automatically.\n\n"
            "Get a free key with starter credits at "
            "[console.anthropic.com](https://console.anthropic.com)."
        )

    with t2:
        st.markdown("**Groq (free hosted)** — free tier, no local server needed, works on Streamlit Cloud")
        if _groq_api_key():
            st.success(f"✅ Active — model: `{_groq_model()}` at `{_groq_base_url()}`")
        else:
            st.info("Not configured")
            st.markdown(
                "To enable:\n"
                "1. Get a free key at [console.groq.com](https://console.groq.com)\n"
                "2. Add to secrets:\n"
                "```toml\n"
                f'FREE_LLM_API_KEY = "gsk_..."\n'
                f'FREE_LLM_BASE_URL = "https://api.groq.com/openai/v1"\n'
                f'FREE_LLM_MODEL = "{DEFAULT_GROQ_MODEL}"\n'
                "```\n"
                "Unlike Ollama, this works from a Streamlit Cloud deployment, not just your own machine — "
                "it's a real remote API call, just a free-tier one."
            )

    with t3:
        st.markdown("**Ollama (local)** — free, private, works offline, requires local install")
        if _ollama_available():
            st.success(f"✅ Running at `{_ollama_url()}` — model: `{DEFAULT_OLLAMA_MODEL}`")
        else:
            st.info("Not detected")
            st.markdown(
                "To enable locally:\n"
                "```bash\n"
                "# Install Ollama: https://ollama.com\n"
                "ollama pull llama3      # or mistral, phi3, codellama\n"
                "ollama serve            # starts at localhost:11434\n"
                "```\n"
                "⚠️ Ollama only works when it's running on the SAME machine as this app "
                "(e.g. your laptop during local dev). On Streamlit Cloud, 'localhost' is the "
                "cloud server, not your laptop — use Tier 1 or Tier 2 there instead."
            )

    with t4:
        st.markdown("**Rule-based fallback** — always available, no API key or local server needed")
        st.success("✅ Always active as final fallback")
        st.markdown(
            "Covers the most common Spark/Delta error patterns:\n"
            "- Delta MERGE concurrency conflicts\n"
            "- AnalysisException (column not found)\n"
            "- OutOfMemoryError (skewed shuffle)\n"
            "- Timeout errors\n"
            "- SCD Type 2 merge patterns\n"
            "- Deduplication patterns"
        )

with col2:
    st.markdown("**Currently active**")
    tier_icons = {1: "🟢", 2: "🔵", 3: "🟡", 4: "🔴"}
    st.markdown(
        f"### {tier_icons.get(status['tier'], '⚪')} Tier {status['tier']}\n"
        f"**{status['provider']}**\n\n"
        f"{status['note']}"
    )
    if status.get("forced_mode"):
        st.caption(f"⚠️ Forced to `{status['forced_mode']}` for this session — clear below to go back to auto.")

    st.divider()
    st.markdown("**Force a tier (this session only)**")
    st.caption(
        "By default the app auto-picks the best available tier (Claude > Groq > Ollama > "
        "rule-based) — with a Claude key configured, Claude always wins. Force a lower tier "
        "here to actually test it, without removing your key or touching secrets.toml."
    )
    _mode_labels = {
        "auto": "Auto (best available)",
        "claude": "Force Claude API",
        "groq": "Force Groq (free hosted)",
        "ollama": "Force Ollama (local)",
        "rule_based": "Force rule-based fallback",
    }
    _current_mode = _provider_mode()
    _chosen_label = st.selectbox(
        "Provider mode",
        options=list(_mode_labels.values()),
        index=list(_mode_labels.keys()).index(_current_mode),
    )
    _chosen_mode = [k for k, v in _mode_labels.items() if v == _chosen_label][0]
    if _chosen_mode != _current_mode:
        set_session_provider_mode(_chosen_mode)
        st.rerun()

# ── RAG Memory ─────────────────────────────────────────────────────────────
st.divider()
st.subheader("RAG Memory")

stats = memory_stats()
c1, c2, c3 = st.columns(3)
c1.metric("Incident memory", f"{stats['incident_count']} incidents", help="Resolved pipeline failures stored for retrieval")
c2.metric("Knowledge base", f"{stats['knowledge_chunks']} chunks", help="Uploaded docs, schemas, runbooks")
c3.metric("Embedding model", stats["embedding_model"])

st.markdown(
    f"- **Primary backend:** {stats['primary_backend']}\n"
    f"- **Fallback backend:** {stats['fallback_backend']}\n"
    f"- **Paid API required:** {'Yes' if stats['paid_api_required'] else 'No'}"
)

col_a, col_b = st.columns(2)
with col_a:
    if st.button("🗑️ Clear knowledge base", type="secondary"):
        if st.session_state.get("confirm_clear_kb"):
            clear_knowledge()
            st.session_state.pop("confirm_clear_kb")
            st.success("Knowledge base cleared.")
            st.rerun()
        else:
            st.session_state["confirm_clear_kb"] = True
            st.warning("Click again to confirm — this cannot be undone.")
with col_b:
    st.caption(
        "Incident memory is never auto-cleared — it accumulates "
        "as the agent resolves failures and learns from them."
    )

# ── Databricks execution ─────────────────────────────────────────────────────
st.divider()
st.subheader("Databricks execution")

from core.databricks_executor import configured as _databricks_configured, test_connection as _databricks_test

if _databricks_configured():
    st.success("✅ `DATABRICKS_HOST` / `DATABRICKS_TOKEN` are set — real pipeline runs will use Databricks serverless PySpark.")
else:
    st.info("Not configured — pipelines run locally (verified pandas on your real uploaded data, or simulation).")

if st.button("🔌 Test Databricks connection", disabled=not _databricks_configured()):
    with st.spinner("Checking authentication — no job is submitted, no file is uploaded..."):
        result = _databricks_test()
    if result["success"]:
        st.success(f"✅ Connected as `{result['user']}` at `{result['host']}`")
    else:
        st.error(f"❌ {result['reason']}")

st.markdown(
    "**Free Edition vs. paid workspace:** there is no separate code path — both speak the "
    "same Databricks REST API, so this app behaves identically either way. Which one you get "
    "depends only on which workspace `DATABRICKS_HOST` points to:\n"
    "- **Free Edition** — sign up at [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition) "
    "for a personal workspace with serverless compute at no cost, usage-limited.\n"
    "- **Paid workspace** — your organization's Databricks account, no usage limits.\n\n"
    "```toml\n"
    "DATABRICKS_HOST = \"https://your-workspace.cloud.databricks.com\"\n"
    "DATABRICKS_TOKEN = \"dapi...\"\n"
    "```\n"
    "Add these to `.streamlit/secrets.toml` locally, or **⋮ → Settings → Secrets** on Streamlit Cloud. "
    "Never paste a token into chat, a script, or source control — this app only ever reads it from secrets/env."
)

# ── Deployment notes ────────────────────────────────────────────────────────
st.divider()
st.subheader("Deployment")
st.markdown(
    "**Streamlit Community Cloud (free):**\n"
    "1. Push this repo to GitHub\n"
    "2. Go to [share.streamlit.io](https://share.streamlit.io) → New app → pick repo → `app.py`\n"
    "3. Deploy. First boot ~3-5 min (installs dependencies)\n"
    "4. Add `ANTHROPIC_API_KEY` in Settings → Secrets for live AI\n\n"
    "**Persistence:** Streamlit Cloud resets the filesystem on redeploy. "
    "Incident memory and the knowledge base will be lost on restart. "
    "For durable storage, swap `core/store.py` and `core/rag_memory.py` "
    "for a Supabase Postgres + Pinecone free-tier backend (no code changes elsewhere needed)."
)
