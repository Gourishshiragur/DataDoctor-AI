# DataDoctor AI

Enterprise-grade, agentic Lakehouse pipeline ops console that demonstrates the full
modern GenAI + Data Engineering skill stack — built for deployment on Streamlit Community
Cloud at zero cost.

[![Streamlit](https://img.shields.io/badge/Streamlit-1.38-FF4B4B)](https://streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB)](https://python.org)
[![Claude API](https://img.shields.io/badge/Claude-API-000000)](https://anthropic.com)

---

## Skills demonstrated

| Skill | Where |
|---|---|
| **Agentic AI (observe→reason→act)** | `core/agent.py` — multi-step loop, not a single prompt |
| **RAG (Retrieval-Augmented Generation)** | `core/rag_memory.py` — semantic search over past incidents |
| **Vector database (ChromaDB)** | `core/rag_memory.py` — persistent in-process vector store |
| **Embeddings (sentence-transformers)** | `core/rag_memory.py` — `all-MiniLM-L6-v2` model |
| **Tool calling / function calling** | `core/agent.py` — `search_past_incidents`, `explain_error`, `suggest_optimization` |
| **Streaming LLM responses** | `core/agent.py` `diagnose_stream()` + `pages/7_Chat.py` |
| **Multi-turn conversation** | `pages/7_Chat.py` — full message history + RAG context injection |
| **LLM API integration** | `core/ai_assistant.py` + `core/agent.py` — Claude API |
| **Data pipeline orchestration** | `core/pipeline_engine.py` — DAG, dependency ordering, retry |
| **Delta Lake / Medallion architecture** | Templates, agent system prompt, code suggestions |
| **PySpark / SCD2 / streaming patterns** | Templates, code assistant, agent fix suggestions |

All GenAI features degrade gracefully: no API key → rule-based fallback + local vector store.
The app is fully functional and demoable at zero cost.

---

## Features

### 🛠️ Pipeline Builder
Visual step composer: source → transform → sink steps with explicit DAG dependencies.
One-click templates: Bronze/Silver/Gold batch, idempotent micro-batch upsert, SCD2
dimension load, SQL-only aggregation.

### 📊 Monitor & Repair
Per-run step status, retry a single failed step, or "Repair run" to re-execute only
failed/skipped steps while leaving succeeded steps untouched — same pattern as Databricks
Jobs "repair run" and ADF rerun-from-failure.

### 📈 Analytics
Run trends over time, success rate, failure reasons auto-grouped by exception type,
per-pipeline reliability scoring.

### 🧠 AI Agent Console ← core GenAI feature
Full agentic loop with RAG, tool calling, and streaming:

1. **OBSERVE** — gathers failed step's code, error, logs + retrieves semantically similar
   past incidents from the RAG vector store (ChromaDB + sentence-transformers)
2. **REASON** — Claude API with tool calling: the model invokes `search_past_incidents`,
   `explain_error`, and `suggest_optimization` as needed before producing a diagnosis
3. **ACT** — on confirmation, patches the step code + retries + stores the resolved incident
   back to RAG memory (so future diagnoses get richer context over time)

Responses stream token-by-token. Confidence levels (high/medium/low) are surfaced clearly.

### 💬 Conversational AI
Multi-turn chat with persistent message history. Each user message triggers RAG retrieval —
relevant past incidents are injected as system context before the LLM responds. Streaming
responses. Covers Spark, Delta Lake, SCD2, ADF, streaming architecture questions.

### 🤖 AI Code Assistant
Single-turn PySpark/SQL snippet generation with common pattern library as local fallback.

### 📜 Logs
Structured, filterable per-run execution logs with severity levels. Downloadable as JSONL.

---

## Free deployment on Streamlit Community Cloud

### Step 1 — Push to GitHub

```bash
cd datadoctor-ai
git init
git add .
git commit -m "DataDoctor AI — initial deploy"
# Create a new repo on GitHub (can be public or private)
git remote add origin https://github.com/YOUR_USERNAME/datadoctor-ai.git
git push -u origin main
```

### Step 2 — Deploy on Streamlit Community Cloud

1. Go to **[share.streamlit.io](https://share.streamlit.io)** — sign in with GitHub
2. Click **"New app"**
3. Set:
   - Repository: `YOUR_USERNAME/datadoctor-ai`
   - Branch: `main`
   - Main file path: `app.py`
4. Click **"Deploy"** — Streamlit installs `requirements.txt` automatically

First deploy takes ~3-5 minutes (sentence-transformers model downloads on first AI Agent use).

### Step 3 — Add your API key (optional but recommended)

In your deployed app: **Settings (⋮) → Secrets**, add:

```toml
ANTHROPIC_API_KEY = "sk-ant-..."
```

Without this: the app runs fully with rule-based fallbacks + local vector store.
With this: Claude API powers live diagnosis, code suggestions, and chat.

You can get a free API key at **[console.anthropic.com](https://console.anthropic.com)** —
new accounts include free credits.

### Step 4 — Your public URL

Your app is now live at:
```
https://YOUR_USERNAME-datadoctor-ai-app-XXXXX.streamlit.app
```

Add this URL to your portfolio site's `js/data.js` under the DataDoctor AI project's
`demo` field and push — the "Live demo ↗" link appears automatically.

---

## Local setup (for testing before deploy)

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Optional: add API key for live AI features
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
# Edit .streamlit/secrets.toml: ANTHROPIC_API_KEY = "sk-ant-..."

streamlit run app.py
# Opens at http://localhost:8501
```

---

## Testing the app (step-by-step demo script)

### 1. Create and run a pipeline that will fail

- Go to **Pipeline Builder**
- Click "Use template" → **Idempotent micro-batch upsert**
- Click "Save pipeline", give it a name (e.g. "telemetry-ingest")
- From the "Run a saved pipeline" dropdown, run it
- Some steps will fail (the engine intentionally simulates realistic failures)

### 2. Monitor and repair

- Go to **Monitor & Repair**
- You'll see the failed run with per-step status
- Try "Repair run" — re-runs only failed/skipped steps, leaves succeeded ones untouched

### 3. AI Agent diagnosis (the main feature)

- Go to **AI Agent Console**
- Select a failed step from the dropdown
- Click "Diagnose with agent" with streaming ON
  - Watch tokens arrive in real time
  - See RAG memory hits if any similar incidents exist
  - See which tools the agent called (search, explain, suggest)
- Click "Apply fix, retry & learn"
  - The step is retried with the proposed fix
  - The incident is stored to RAG memory

### 4. RAG memory in action (run a second pipeline)

- Create and run another pipeline with a similar error
- Go back to AI Agent → the RAG panel will now show past similar incidents
- The diagnosis prompt will include that retrieved context — this is the RAG loop working

### 5. Conversational AI

- Go to **💬 Conversational AI**
- Ask: "How do I implement SCD Type 2 with Spark Structured Streaming?"
- Ask: "I got an AnalysisException: cannot resolve column 'device_id'. What's wrong?"
- With RAG toggle ON — if you have incidents stored, they appear as injected context

### 6. AI Code Assistant

- Go to **🤖 AI Code Assistant**
- Try "deduplicate on natural key keeping the latest event time"
- Try "SCD Type 2 merge to close and open a version on status change"
- Click the example buttons

---

## Architecture

```
app.py  (Dashboard)
├── pages/
│   ├── 1_Pipeline_Builder.py      DAG composer + templates
│   ├── 2_Monitor_and_Repair.py    Run monitor, retry, repair
│   ├── 3_AI_Code_Assistant.py     PySpark/SQL snippet gen
│   ├── 5_AI_Agent.py              Agentic loop UI (RAG + tools + streaming)
│   ├── 6_Analytics.py             Run trends + reliability
│   └── 7_Chat.py                  Multi-turn conversational AI
└── core/
    ├── models.py                   Pipeline/Step/Run dataclasses
    ├── store.py                    JSON file persistence
    ├── pipeline_engine.py          DAG execution engine (simulated)
    ├── retry_engine.py             Retry + repair logic
    ├── templates.py                One-click pipeline templates
    ├── rag_memory.py               ← RAG: ChromaDB + embeddings (cosine fallback)
    ├── agent.py                    ← Agentic loop: tools + streaming + RAG
    ├── ai_assistant.py             Code snippet generation
    └── ui.py                       Shared styles + status badges
```

## Why simulated execution

`core/pipeline_engine.py` simulates step execution rather than running a real Spark cluster —
a free-tier web app cannot host Spark. The orchestration, retry, repair, RAG, and agent
mechanics behave exactly as they would against a real backend. To connect to a real Databricks
cluster, replace `_execute_step()` in `pipeline_engine.py` with a Databricks Jobs API call —
every other module is unchanged.

## Notes on persistence

Streamlit Community Cloud's filesystem resets on redeploy/restart — pipelines, run history,
and RAG memory built up in the UI will be lost. This is expected and noted in the UI. For
durable storage, swap `core/store.py` for a Supabase Postgres free tier and `core/rag_memory.py`
for a hosted vector DB (Pinecone free tier, Weaviate Cloud free tier) — neither requires
changes to any other module.
