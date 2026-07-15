"""
DataDoctor AI — Settings & Configuration

Configure which LLM powers the AI features, view RAG memory status,
and manage the knowledge base.
"""
import streamlit as st

from core.llm_provider import (
    CLAUDE_MODEL,
    DEFAULT_OLLAMA_MODEL,
    DEFAULT_OLLAMA_URL,
    _claude_api_key,
    _ollama_available,
    llm_status,
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
tier_color = {"🟢": 1, "🟡": 2, "🔴": 3}

col1, col2 = st.columns([2, 1])
with col1:
    t1, t2, t3 = st.tabs(["Tier 1 — Claude API", "Tier 2 — Ollama (local)", "Tier 3 — Fallback"])

    with t1:
        st.markdown("**Claude API (Anthropic)** — best quality, works on Streamlit Cloud")
        if _claude_api_key():
            st.success(f"✅ Active — model: `{CLAUDE_MODEL}`")
        else:
            st.warning("Not configured")
            st.markdown(
                "To enable:\n"
                "1. Go to your app on **share.streamlit.io**\n"
                "2. Click **⋮ → Settings → Secrets**\n"
                "3. Add:\n"
                "```toml\nANTHROPIC_API_KEY = \"sk-ant-...\"\n```\n"
                "4. Save — the app restarts automatically.\n\n"
                "Get a free key with starter credits at "
                "[console.anthropic.com](https://console.anthropic.com)."
            )

    with t2:
        st.markdown("**Ollama (local)** — free, private, works offline, requires local install")
        if _ollama_available():
            st.success(f"✅ Running at `{DEFAULT_OLLAMA_URL}` — model: `{DEFAULT_OLLAMA_MODEL}`")
        else:
            st.info("Not detected")
            st.markdown(
                "To enable locally:\n"
                "```bash\n"
                "# Install Ollama: https://ollama.com\n"
                "ollama pull llama3      # or mistral, phi3, codellama\n"
                "ollama serve            # starts at localhost:11434\n"
                "```\n"
                "⚠️ Ollama is not available on Streamlit Cloud — use Tier 1 for cloud deployment."
            )

    with t3:
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
    tier_icons = {1: "🟢", 2: "🟡", 3: "🔴"}
    st.markdown(
        f"### {tier_icons.get(status['tier'], '⚪')} Tier {status['tier']}\n"
        f"**{status['provider']}**\n\n"
        f"{status['note']}"
    )

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
