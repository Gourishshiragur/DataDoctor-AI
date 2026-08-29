"""ui/About.py — project overview, architecture, and mode explanation."""
import streamlit as st


def render():
    st.title("ℹ️ About DataDoctorAI")

    st.markdown(
        """
DataDoctorAI is a self-healing data pipeline platform: upload raw data (or point it at
Databricks + Unity Catalog) and watch it flow live through **Bronze → Silver → Gold**,
with an AI-assisted repair engine automatically fixing nulls, duplicates, outliers, and
inconsistent categories as it goes — every step streamed to the screen in real time.

### Architecture
- **Bronze** — raw, as-ingested data plus lineage metadata.
- **Silver** — quality-checked and self-healed data (rule-based repair engine, optionally
  narrated by whichever AI provider you've configured).
- **Gold** — business-ready aggregates powering the Dashboard and Business AI chat/SQL tools.

### Demo Mode vs Enterprise Mode
Demo Mode runs entirely offline: a local DuckDB file stands in for the warehouse, and a
deterministic rule-based responder stands in for AI when no provider is configured — so
every feature works with zero setup. Configure Databricks + Unity Catalog in **Settings**
and the exact same UI switches to Enterprise Mode with zero code changes: the same
pipeline modules write to real Unity Catalog tables instead of the local file.

### AI Provider Priority
1. Ollama (local)
2. Gemini
3. OpenRouter
4. OpenAI
5. Azure OpenAI
6. Claude

The app automatically uses the first configured, reachable provider — set these once
under **Settings**.
        """
    )

    st.divider()
    st.caption("Built as a reference architecture — extend pipeline/, ai/, and databricks/ modules for your own data.")
