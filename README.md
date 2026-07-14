# DataDoctor AI

Enterprise-grade, agentic Lakehouse pipeline ops console that demonstrates the full
modern GenAI + Data Engineering skill stack — built for deployment on Streamlit Community
Cloud at zero cost.

[![Streamlit](https://img.shields.io/badge/Streamlit-1.38-FF4B4B)](https://streamlit.io)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB)](https://python.org)
[![Claude API](https://img.shields.io/badge/Claude-API-000000)](https://anthropic.com)

---

## Skills Demonstrated

| Skill                                         | Implementation                                                                         |
| --------------------------------------------- | -------------------------------------------------------------------------------------- |
| **Agentic AI (Observe → Reason → Act)**       | `core/agent.py` — multi-step reasoning and action loop rather than a single LLM prompt |
| **RAG (Retrieval-Augmented Generation)**      | `core/rag_memory.py` — semantic retrieval over previously resolved pipeline incidents  |
| **Vector Database (ChromaDB)**                | `core/rag_memory.py` — persistent in-process vector store                              |
| **Embeddings (Sentence Transformers)**        | `core/rag_memory.py` — `all-MiniLM-L6-v2` embedding model                              |
| **Tool / Function Calling**                   | `core/agent.py` — `search_past_incidents`, `explain_error`, and `suggest_optimization` |
| **Streaming LLM Responses**                   | `core/agent.py` `diagnose_stream()` + `pages/7_Chat.py`                                |
| **Multi-Turn Conversation**                   | `pages/7_Chat.py` — conversation history with RAG context injection                    |
| **LLM API Integration**                       | `core/ai_assistant.py` + `core/agent.py` — Claude API integration                      |
| **Data Pipeline Orchestration**               | `core/pipeline_engine.py` — DAG execution, dependency ordering, and retry handling     |
| **Delta Lake / Medallion Architecture**       | Pipeline templates, agent context, and generated code suggestions                      |
| **PySpark / SCD Type 2 / Streaming Patterns** | Pipeline templates, AI code assistant, and agent-generated remediation suggestions     |

All Generative AI capabilities degrade gracefully. When an API key is unavailable, the application uses rule-based fallbacks and local vector retrieval, allowing the complete workflow to remain functional and demoable at zero cost.

---

## Features

### 🛠️ Pipeline Builder

Visual pipeline-step composer supporting source, transformation, and sink stages with explicit DAG dependencies.

Includes one-click templates for:

* Bronze–Silver–Gold batch processing
* Idempotent micro-batch upserts
* SCD Type 2 dimension processing
* SQL-based aggregations

### 📊 Monitor & Repair

Provides per-run and per-step execution status.

Users can retry an individual failed step or use **Repair Run** to re-execute only failed and skipped steps while preserving successfully completed steps. This demonstrates a recovery pattern similar to Databricks Jobs repair runs and Azure Data Factory rerun-from-failure workflows.

### 📈 Analytics

Provides operational pipeline metrics, including:

* Execution trends over time
* Pipeline success rates
* Failure reasons grouped by exception type
* Per-pipeline reliability scoring

### 🧠 AI Agent Console — Core Generative AI Feature

Implements an agentic workflow using RAG, tool calling, streaming responses, and human confirmation.

1. **OBSERVE** — Collects the failed step's code, exception details, and execution logs. It also retrieves semantically similar historical incidents from the RAG vector store using ChromaDB and Sentence Transformers.

2. **REASON** — Uses the Claude API with tool calling. The agent can invoke `search_past_incidents`, `explain_error`, and `suggest_optimization` before generating a diagnosis and recommended remediation.

3. **ACT** — After user confirmation, the agent applies the proposed code patch, retries the failed step, and stores the resolved incident in RAG memory so future diagnoses can use the newly learned context.

Responses are streamed token by token, and diagnosis confidence is surfaced as **High**, **Medium**, or **Low**.

### 💬 Conversational AI

Multi-turn AI assistant with conversation history and RAG context injection.

For every user query, relevant historical pipeline incidents are retrieved and added to the LLM context before response generation.

The assistant supports discussions related to:

* Apache Spark
* PySpark
* Delta Lake
* SCD Type 2
* Azure Data Factory
* Batch and streaming architectures
* Pipeline failures and optimization

### 🤖 AI Code Assistant

Generates PySpark and SQL code snippets using an LLM when an API key is available and a local pattern library as a fallback.

Supported patterns include:

* Data deduplication
* Incremental processing
* Delta MERGE
* SCD Type 2
* Streaming transformations
* Data-quality validation

### 📜 Execution Logs

Provides structured and filterable pipeline execution logs with severity levels.

Logs can be exported in JSON Lines (`.jsonl`) format for further analysis.

---

## Deploy on Streamlit Community Cloud

1. Go to **[share.streamlit.io](https://share.streamlit.io)** and sign in using GitHub.

2. Click **New app**.

3. Configure the deployment:

   * Repository: `Gourishshiragur/DataDoctor-AI`
   * Branch: `main`
   * Main file path: `app.py`

4. Click **Deploy**.

Streamlit Community Cloud automatically installs the dependencies defined in `requirements.txt`.

The first deployment may take approximately **3–5 minutes** because the Sentence Transformers embedding model is downloaded during initial use.

### Configure the Claude API Key — Optional

In the deployed Streamlit application, open:

**Settings (⋮) → Secrets**

Add:

```toml
ANTHROPIC_API_KEY = "your-api-key"
```

Without an API key, the application remains functional using rule-based AI fallbacks and local vector retrieval.

With an API key, the Claude API enables live agent diagnosis, contextual code suggestions, and conversational AI responses.

Create and manage API keys through the Anthropic Console.

### Add the Live Application to Your Portfolio

After deployment, copy the generated Streamlit application URL.

Add the URL to the DataDoctor AI project's `demo` field in your portfolio configuration and push the update to GitHub. The **Live Demo ↗** link will then appear in the portfolio.

---

## Local Setup

Clone the repository:

```bash
git clone https://github.com/Gourishshiragur/DataDoctor-AI.git
cd DataDoctor-AI
```

Create and activate a Python virtual environment:

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Install the required dependencies:

```bash
pip install -r requirements.txt
```

Optional: configure the Claude API key for live AI capabilities:

```bash
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Update `.streamlit/secrets.toml`:

```toml
ANTHROPIC_API_KEY = "your-api-key"
```

Start the application:

```bash
streamlit run app.py
```

The application will be available locally at:

```text
http://localhost:8501
```

---

## Testing the Application

### 1. Create and Run a Pipeline

* Open **Pipeline Builder**.
* Select **Use Template → Idempotent Micro-Batch Upsert**.
* Click **Save Pipeline**.
* Enter a pipeline name, such as `telemetry-ingest`.
* Select the saved pipeline and start a run.
* The execution engine may intentionally generate a realistic pipeline failure for demonstration.

### 2. Monitor and Repair the Pipeline

* Open **Monitor & Repair**.
* Review the execution status of each pipeline step.
* Select **Repair Run**.
* Only failed and skipped steps are re-executed; successfully completed steps remain unchanged.

### 3. Diagnose a Failure with the AI Agent

* Open **AI Agent Console**.
* Select a failed pipeline step.
* Enable response streaming.
* Click **Diagnose with Agent**.
* Review:

  * The streamed diagnosis
  * Similar incidents retrieved through RAG
  * Tools invoked by the agent
  * Root-cause analysis
  * Recommended remediation
  * Diagnosis confidence

Click **Apply Fix, Retry & Learn**.

The application then:

* Applies the proposed remediation
* Retries the failed pipeline step
* Stores the resolved incident in RAG memory

### 4. Validate RAG Memory

* Create and run another pipeline with a similar failure.
* Return to **AI Agent Console**.
* Review the retrieved historical incidents in the RAG panel.
* The previously resolved incident is injected into the diagnosis context.

This demonstrates the complete retrieval and learning workflow:

```text
Pipeline Failure
       ↓
Incident Embedding
       ↓
Vector Storage
       ↓
Semantic Retrieval
       ↓
Context-Augmented Diagnosis
       ↓
Resolution Stored for Future Retrieval
```

### 5. Test Conversational AI

Open **💬 Conversational AI** and try:

```text
How do I implement SCD Type 2 with Spark Structured Streaming?
```

```text
I received an AnalysisException because Spark cannot resolve the device_id column. What could be wrong?
```

When RAG is enabled, relevant stored incidents are retrieved and injected into the conversation context.

### 6. Test the AI Code Assistant

Open **🤖 AI Code Assistant** and try:

```text
Deduplicate records using a natural key while retaining the latest event.
```

```text
Generate an SCD Type 2 Delta MERGE to close the previous record and create a new version when the status changes.
```

---

## Architecture

```text
app.py                                  Streamlit dashboard
│
├── pages/
│   ├── 1_Pipeline_Builder.py           DAG composer and pipeline templates
│   ├── 2_Monitor_and_Repair.py         Run monitoring, retry, and repair
│   ├── 3_AI_Code_Assistant.py          PySpark and SQL generation
│   ├── 5_AI_Agent.py                   Agentic AI workflow with RAG and tools
│   ├── 6_Analytics.py                  Pipeline trends and reliability metrics
│   └── 7_Chat.py                       Multi-turn conversational AI
│
└── core/
    ├── models.py                       Pipeline, step, and run data models
    ├── store.py                        JSON-based application persistence
    ├── pipeline_engine.py              Simulated DAG execution engine
    ├── retry_engine.py                 Step retry and repair logic
    ├── templates.py                    Reusable pipeline templates
    ├── rag_memory.py                   ChromaDB, embeddings, and retrieval
    ├── agent.py                        Agent loop, tools, streaming, and RAG
    ├── ai_assistant.py                 AI-assisted code generation
    └── ui.py                           Shared UI styles and status components
```

---

## Why Pipeline Execution Is Simulated

`core/pipeline_engine.py` simulates pipeline-step execution rather than starting a real Apache Spark cluster.

Streamlit Community Cloud is designed for lightweight web applications and cannot host a production Databricks or distributed Spark environment. The project therefore focuses on demonstrating the operational control layer:

* DAG-based orchestration
* Dependency management
* Pipeline monitoring
* Step-level retry
* Repair-run behavior
* AI-assisted root-cause analysis
* RAG-based incident retrieval
* Tool-assisted remediation
* Human-approved recovery workflows

The execution backend is intentionally abstracted.

To integrate the application with Azure Databricks, replace the simulated `_execute_step()` implementation in `core/pipeline_engine.py` with Databricks Jobs API calls. The monitoring, retry, repair, RAG, and agent layers can remain largely unchanged.

---

## Persistence Limitations

Streamlit Community Cloud uses an ephemeral filesystem.

Pipeline definitions, execution history, logs, and RAG incidents created through the application may be lost when the application restarts or is redeployed.

This limitation is documented in the application UI.

For durable production persistence:

* Replace the JSON implementation in `core/store.py` with PostgreSQL or another managed database.
* Replace the local vector store in `core/rag_memory.py` with a managed vector database.
* Store secrets using a secure secrets manager.
* Add authentication and role-based access control.
* Integrate with a production pipeline execution platform.

These changes can be implemented without redesigning the application's agent, monitoring, or recovery workflows.

---

## Project Scope

DataDoctor AI is a portfolio and architecture demonstration project. It is designed to showcase the integration of:

* Data Engineering
* Pipeline orchestration
* Agentic AI
* Retrieval-Augmented Generation
* Vector search
* LLM tool calling
* Streaming AI responses
* Automated diagnosis
* Human-in-the-loop remediation
* Operational analytics

The application demonstrates an extensible architecture for an AI-assisted DataOps platform while remaining deployable using free-tier infrastructure.

