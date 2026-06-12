# Databricks Pipeline Observability — MCP Server + Agent

A natural language interface for Databricks lakehouse health monitoring.  
Ask questions in plain English → LangGraph agent calls MCP tools → structured diagnosis.

---

## Architecture

```
User query (plain English)
    │
    ▼
LangGraph ReAct Agent  (Groq llama-3.3-70b-versatile)
    │  reasons over tool results, max 3 rounds
    │
    ▼  stdio (spawns subprocess, communicates via stdin/stdout)
FastMCP Server  (server.py)
    │
    ├── get_job_run_status     → job run history, failures, error messages
    ├── get_cluster_health     → CPU / memory / DBU utilisation per cluster
    ├── get_dlt_health         → DLT pipeline state, backlog, errors
    ├── get_table_stats        → Delta file count, fragmentation, OPTIMIZE recency
    ├── get_recent_queries     → failed / slow SQL queries from warehouse history
    └── get_lineage            → upstream / downstream table dependencies
    │
    ▼
mock_data.py  (swap for real Databricks SDK calls to go live)
    │
    ▼
Structured diagnosis:
  ## What Happened / ## Root Cause / ## Blast Radius / ## Recommended Fix
```

---

## Stack

| Layer | Library |
|---|---|
| LLM | `langchain-groq` — Groq llama-3.3-70b-versatile |
| Agent | `langgraph` — ReAct prebuilt graph |
| MCP client | `langchain-mcp-adapters` — MultiServerMCPClient |
| MCP server | `fastmcp` — stdio transport |
| Tracing | `langsmith` |
| Config | `python-dotenv` |
| Tests | `pytest` |

---

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/AkramHussainChoudhury/MCP.git
cd MCP
```

### 2. Create a virtual environment (Python 3.10+)

```bash
python -m venv venv
source venv/bin/activate        # Linux / Mac
# or
.\venv\Scripts\Activate.ps1    # Windows PowerShell
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

```bash
cp .env.example .env
```

Open `.env` and add your keys:

```
GROQ_API_KEY=your_groq_api_key_here          # required — get from console.groq.com/keys

LANGCHAIN_TRACING_V2=true                    # optional — LangSmith tracing
LANGCHAIN_API_KEY=your_langsmith_key_here    # optional — get from smith.langchain.com
LANGCHAIN_PROJECT=databricks-observability   # optional — project name in LangSmith
```

### 5. Run the agent

```bash
# Default query
python agent.py

# Custom query
python agent.py "why did the aggregate_gold job fail this morning?"
python agent.py "which tables urgently need OPTIMIZE?"
python agent.py "what failed in the last hour and what is the blast radius?"
python agent.py "is the bronze_to_silver DLT pipeline healthy?"
python agent.py "what is the blast radius if silver.events is down?"
```

You do **not** need to start `server.py` separately — the agent spawns it automatically.

### 6. Run the tests

```bash
pytest tests/ -v
```

---

## Example Output

```
Query: Why did the aggregate_gold job fail this morning and what is the blast radius?
────────────────────────────────────────────────────────────

## What Happened
The aggregate_gold job (run 88421) failed at 03:30 UTC on 2026-06-09 with a
Java OutOfMemoryError in shuffle stage 14, caused by GC overhead on cluster
etl-main-prod (0601-084523-reef412).

## Root Cause
Cluster etl-main-prod is at 93.7% memory utilisation with 18.4% GC time.
The immediate trigger is prod_catalog.silver.events having 28,470 files
(fragmentation score 0.81) — last OPTIMIZE was 30 days ago. The high file
count caused 894 tasks in the shuffle stage, exhausting executor heap memory.

## Blast Radius
silver.events feeds 3 gold tables (kpi_daily, funnel_analysis, retention_cohorts)
and the ML feature store (ml_catalog.features.user_event_features).
Blast radius score: 8/10. Executive Tableau and ops Superset dashboards are
now reading stale data.

## Recommended Fix
1. Run OPTIMIZE on prod_catalog.silver.events immediately to compact 28k files.
2. Increase executor memory or enable autoscaling on etl-main-prod (currently
   capped at 16 workers).
3. Re-run the aggregate_gold job after OPTIMIZE completes.
4. Schedule weekly OPTIMIZE + VACUUM on silver.events via a Databricks job.
```

---

## Project Structure

```
MCP/
├── mock_data.py          # Data layer — mirrors Databricks REST API response schemas
├── server.py             # FastMCP server — exposes 6 tools via stdio
├── agent.py              # LangGraph ReAct agent — consumes MCP server
├── tests/
│   ├── __init__.py
│   ├── conftest.py       # Shared IDs and constants
│   ├── test_cluster_health.py
│   ├── test_dlt_health.py
│   ├── test_table_stats.py
│   ├── test_recent_queries.py
│   └── test_lineage.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Going Live with Real Databricks

Every function in `mock_data.py` maps to a real Databricks REST endpoint.
Replace the function body — nothing else changes.

```python
# mock_data.py — before (mock)
def get_job_run_status(job_name=None):
    return {"runs": [...hardcoded...]}

# mock_data.py — after (real)
from databricks.sdk import WorkspaceClient

def get_job_run_status(job_name=None):
    w = WorkspaceClient()    # reads DATABRICKS_HOST + DATABRICKS_TOKEN from env
    runs = w.jobs.list_runs(limit=10, expand_tasks=True)
    return {"runs": [r.as_dict() for r in runs]}
```

`server.py` and `agent.py` require no changes.

---

## Design Decisions

**Why stdio transport?**  
The agent and server run on the same machine. Stdio is the canonical MCP transport used by Claude Desktop, Cursor, and VS Code Copilot — the agent spawns `server.py` as a child process and communicates via stdin/stdout. Switching to a networked HTTP/SSE deployment is one line: `mcp.run(transport="sse", host="0.0.0.0", port=8000)`.

**Why max 3 tool-call rounds?**  
Prevents runaway tool loops and keeps Groq token costs predictable. Enforced via LangGraph's `recursion_limit=8` (each round = 2 node transitions in the ReAct graph).

**Why `temperature=0` on the LLM?**  
Observability queries need deterministic, evidence-based answers — not creative ones.
