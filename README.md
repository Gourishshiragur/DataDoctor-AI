# 🩺 DataDoctorAI

A self-healing data pipeline platform. Upload messy data (or point it at Databricks +
Unity Catalog) and watch it flow live through **Bronze → Silver → Gold**, with an
AI-assisted repair engine automatically fixing nulls, duplicates, outliers, and
inconsistent categories as it goes — every step streamed to the screen in real time.

## Quick start (Demo Mode — zero setup)

```bash
pip install -r requirements.txt
streamlit run app.py
```

Open the app, go to **Pipeline Studio**, pick one of the 5 bundled demo datasets
(retail, banking, healthcare, e-commerce, manufacturing) or upload your own
CSV/Excel/JSON/Parquet file, and click **Run Self-Healing Pipeline**. You'll see:

1. **Bronze** — raw ingestion with lineage metadata
2. **Profiling** — nulls, duplicates, quality score
3. **Silver** — quality checks run live, then the self-healing repair engine fixes
   each issue one at a time, streamed to the screen
4. **Gold** — business aggregates, ready for the Dashboard and Business AI chat
5. **AI pipeline review** — a natural-language recommendation for the Bronze→Silver→Gold
   design of *your* data

No API keys are required for any of this — Demo Mode uses a local DuckDB file for
storage and a deterministic rule-based responder wherever an AI provider would
normally be called (Business AI's chat, SQL generator, and repair explanations all
still work, just without a language model's phrasing).

## Enabling real AI providers

Open **Settings** and add a key for any of: Ollama (local, no key), Gemini,
OpenRouter, OpenAI, Azure OpenAI, or Claude. The app tries them in that priority
order and automatically uses the first one that's configured and reachable —
everywhere in the app, with no code changes.

## Enabling Enterprise Mode (Databricks + Unity Catalog)

Open **Settings → Databricks** and fill in your Workspace URL, Personal Access
Token, and SQL Warehouse HTTP Path (found under the warehouse's Connection Details
in the Databricks UI). Click **Test connection**. Once configured, the exact same
UI switches to writing/reading real Unity Catalog tables instead of the local
DuckDB file — see `docs/architecture.md` for how the storage interface is shared
between `storage/db.py` (Demo) and `databricks/connection.py` (Enterprise).

A ready-to-import Databricks Job (`databricks/jobs/job_config.json` +
`databricks/notebooks/bronze_silver_gold_job.py`) is included for teams that want
the transformation to run natively as a workspace job instead of being driven from
this app.

## Project layout

```
DataDoctorAI/
├── app.py                  # Streamlit entry point
├── config/                 # settings persistence, demo/enterprise mode config
├── ui/                     # one file per page (Dashboard, Pipeline Studio, ...)
├── ai/                     # provider router + per-provider clients + repair engine
├── pipeline/                # bronze/silver/gold, profiling, quality, lineage, replay
├── databricks/              # Enterprise-mode connector, Unity Catalog helpers, job template
├── datasets/                 # 5 bundled dirty demo datasets
├── rag/                     # lightweight retrieval over docs + run history
├── monitoring/               # alerting derived from run history
├── storage/                  # local DuckDB engine (Demo Mode warehouse)
├── database/                 # SQLite run/event/lineage/quality/repair history log
├── tests/                    # tests for profiling, quality, repair engine, AI router
└── docs/                      # architecture + dataset documentation (also used by RAG)
```

## Running tests

```bash
python3 tests/test_profiling.py
python3 tests/test_quality.py
python3 tests/test_repair_engine.py
python3 tests/test_provider_router.py
```

All four suites exercise real logic (no mocks) against synthetic and the bundled
demo data.

## Notes & honest limitations

- The Databricks and cloud-AI-provider integrations (`databricks/connection.py`,
  `ai/gemini.py`, `ai/openrouter.py`, and the OpenAI/Azure/Claude calls in
  `ai/provider_router.py`) are written against each service's real REST API, but
  could not be live-tested against actual credentials in this environment — review
  them before pointing at production data, especially `databricks/connection.py`'s
  write path, which is a simple INSERT-based staging approach suitable for small/
  medium batches, not high-volume production loads (use the included Databricks
  Job template for that).
- Gold-layer aggregation is a generic auto-aggregator (groups by the first
  low-cardinality categorical column, sums/averages numeric columns) so it works
  across arbitrary datasets out of the box — for a specific business's real KPIs
  you'll likely want to hand-write the aggregation in `pipeline/gold.py`.
- The RAG in `rag/retriever.py` uses simple keyword-overlap scoring, not embeddings
  — sufficient for grounding answers in this app's own run history and docs, not a
  general-purpose vector search.
