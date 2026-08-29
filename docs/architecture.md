# DataDoctorAI — Architecture

## Layers

**Bronze** (`pipeline/bronze.py`)
Raw ingestion. Never mutates values — only tags rows with `_ingested_at` and a `_row_hash`
for auditability. Written via `storage/db.py` (Demo Mode, local DuckDB) or
`dbx_enterprise/connection.py` (Enterprise Mode, Unity Catalog).

**Silver** (`pipeline/silver.py`)
Runs `pipeline/quality.py` checks (null thresholds, duplicates, invalid negatives, outlier
ranges, categorical consistency), then hands the frame to `ai/repair_engine.py`, the
self-healing core:

1. Drop exact duplicate rows
2. Impute nulls (median for numeric, mode for categorical)
3. Correct invalid negative values (quantity/age/amount/balance/price-like columns)
4. Winsorize numeric outliers to the 1st/99th percentile
5. Normalize inconsistent categorical casing/whitespace

Every action is logged to `database/history.py` (table: `repairs`) with rows-affected
counts, and optionally narrated by whichever AI provider is configured
(`ai/provider_router.py`) for a one-line human-readable rationale.

**Gold** (`pipeline/gold.py`)
Auto-aggregates the Silver frame: picks a low-cardinality categorical column to group by
(if one exists) and computes sum/mean/count over the first few numeric columns. Falls back
to `describe()`-style summary stats when there's no natural grouping column. This keeps
Gold generation dataset-agnostic across retail/banking/healthcare/ecommerce/manufacturing.

## Modes

| | Demo Mode | Enterprise Mode |
|---|---|---|
| Storage | Local DuckDB file (`storage/datadoctor.duckdb`) | Unity Catalog tables via Databricks SQL Warehouse |
| AI | Rule-based offline fallback if no provider configured | Any of Ollama/Gemini/OpenRouter/OpenAI/Azure/Claude |
| Setup | Zero — works immediately | Databricks Workspace URL + PAT + HTTP Path in Settings |

Both modes share the exact same `pipeline/*` call signatures — `write_table`,
`read_table`, `table_exists`, `list_tables` — implemented once in `storage/db.py` and
once in `dbx_enterprise/connection.py`. Switching modes is a Settings-page action, not a
code change.

## Observability

- `database/history.py` — SQLite log of runs, events, lineage edges, quality checks,
  and repairs. Powers the Monitor and Replay pages.
- `pipeline/lineage.py` — renders the lineage log as a Mermaid flowchart for the
  Dashboard.
- `monitoring/alerts.py` — derives alert conditions (persistent quality-check failures,
  failed runs) from recent run history.
- `rag/retriever.py` — lightweight keyword-overlap retrieval over `docs/*.md` and past
  run summaries, used by Business AI to ground "why did X happen" answers.

## Extending

- New dataset domain → drop a CSV in `datasets/<domain>/` and register it in
  `config/demo.py`'s `DEMO_DATASETS`.
- New AI provider → add a `complete()` function in `ai/<provider>.py` and wire it into
  `ai/provider_router.py`'s `infer()` dispatch + `config/settings.py`'s
  `configured_providers()`.
- New quality check → add a function to `pipeline/quality.py` and register it in
  `CHECK_FUNCS`.
- New repair rule → extend `ai/repair_engine.py::repair()`.
