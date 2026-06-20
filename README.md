# Databricks Pipeline Observability — MCP Server + Agent

A natural language interface for Databricks lakehouse health monitoring.  
Ask questions in plain English → LangGraph agent calls MCP tools → structured diagnosis.

Built in two phases:
- **Phase 1** — local mock data, stdio transport (learning / demo)
- **Phase 2** — real Databricks workspace, HTTP/SSE transport, API key auth (production path)

---

## Architecture

```
User query (plain English)
    │
    ▼
LangGraph ReAct Agent  (Groq llama-3.3-70b-versatile)
    │  reasons over tool results, max 3 rounds
    │
    ▼  HTTP/SSE  +  Authorization: Bearer <MCP_API_KEY>
FastMCP Server  (server.py)  — persistent HTTP service on :8000
    │
    ├── get_job_run_status     → job run history, failures, error messages
    ├── get_cluster_health     → CPU / memory / DBU utilisation per cluster
    ├── get_dlt_health         → DLT pipeline state, backlog, errors
    ├── get_table_stats        → Delta file count, fragmentation, OPTIMIZE recency
    ├── get_recent_queries     → failed / slow SQL queries from warehouse history
    └── get_lineage            → upstream / downstream table dependencies
    │
    ▼  auto-selected by DATABRICKS_HOST env var
    ├── mock_data.py        (DATABRICKS_HOST not set → local synthetic data)
    └── databricks_data.py  (DATABRICKS_HOST set     → real Databricks SDK + REST)
              │
              ▼  Authorization: Bearer <DATABRICKS_TOKEN>
         Databricks REST API
    │
    ▼
Structured diagnosis:
  ## What Happened / ## Root Cause / ## Blast Radius / ## Recommended Fix
```

---

## Authentication — Two Layers

```
agent.py ──[MCP_API_KEY]──▶ server.py ──[DATABRICKS_TOKEN]──▶ Databricks API
           client → MCP auth              MCP server → Databricks auth
```

| Token | Purpose | Held by |
|---|---|---|
| `MCP_API_KEY` | Authenticates the agent to the MCP server | Both agent and server `.env` |
| `DATABRICKS_TOKEN` | Authenticates the MCP server to Databricks | Server `.env` only — agent never sees it |

The agent never holds the Databricks token. If the agent is compromised, the attacker cannot call Databricks directly.

---

## Stack

| Layer | Library |
|---|---|
| LLM | `langchain-groq` — Groq llama-3.3-70b-versatile |
| Agent | `langgraph` — ReAct prebuilt graph |
| MCP client | `langchain-mcp-adapters` — MultiServerMCPClient |
| MCP server | `fastmcp` — HTTP/SSE transport |
| Databricks | `databricks-sdk` + `requests` |
| Tracing | `langsmith` |
| Config | `python-dotenv` |
| Tests | `pytest` |

---

## Project Structure

```
MCP/
├── mock_data.py          # Phase 1 — synthetic data mirroring Databricks REST schemas
├── databricks_data.py    # Phase 2 — real Databricks SDK + REST API calls
├── server.py             # FastMCP server — 6 tools, auto-selects data layer
├── agent.py              # LangGraph ReAct agent — connects via HTTP/SSE
├── tests/
│   ├── __init__.py
│   ├── conftest.py
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

---

## Phase 1 — Mock Data (Demo / Learning)

No Databricks account needed. Uses synthetic data with a realistic incident scenario seeded in.

**`.env` — minimum required:**
```
MCP_API_KEY=any-random-string-you-choose
GROQ_API_KEY=your_groq_api_key          # get from console.groq.com/keys
```

**Terminal 1 — start the MCP server:**
```bash
python server.py
# Server starts on http://127.0.0.1:8000
# Serving mock data (DATABRICKS_HOST not set)
```

**Terminal 2 — run the agent:**
```bash
python agent.py
python agent.py "why did the aggregate_gold job fail this morning?"
python agent.py "which tables urgently need OPTIMIZE?"
python agent.py "what failed in the last hour and what is the blast radius?"
python agent.py "is the bronze_to_silver DLT pipeline healthy?"
python agent.py "what is the blast radius if silver.events is down?"
```

---

## Phase 2 — Real Databricks Workspace

**Additional `.env` keys required:**

```
# MCP server auth — generate with:
# python -c "import secrets; print(secrets.token_hex(32))"
MCP_API_KEY=your_generated_key

# Databricks workspace
DATABRICKS_HOST=https://adb-xxxxxxxxxxxx.x.azuredatabricks.net
DATABRICKS_TOKEN=dapi...          # Settings → Developer → Access tokens

# SQL warehouse for DESCRIBE DETAIL / HISTORY queries
DATABRICKS_WAREHOUSE_ID=abc123    # SQL Warehouses → your warehouse → Connection details

# Default catalog and schema when listing all tables (optional)
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default
```

**Terminal 1 — start the MCP server:**
```bash
python server.py
# Server starts on http://127.0.0.1:8000
# Serving real Databricks data (DATABRICKS_HOST is set)
```

**Terminal 2 — run the agent:**
```bash
python agent.py "why did the aggregate_gold job fail this morning?"
```

The server auto-detects `DATABRICKS_HOST` and switches to `databricks_data.py`. No code changes needed.

---

## How the Data Layer Switch Works

`server.py` selects the data layer at startup based on a single env var:

```python
if os.environ.get("DATABRICKS_HOST"):
    import databricks_data as data   # real Databricks SDK calls
else:
    import mock_data as data         # local synthetic data
```

All 6 tool functions call `data.get_*()` — they don't know or care which layer is active.

---

## Real API Mapping (databricks_data.py)

| Tool | Real Databricks API | Notes |
|---|---|---|
| `get_job_run_status` | `w.jobs.list_runs()` | Full SDK support |
| `get_cluster_health` | `w.clusters.list/get()` + `w.clusters.events()` | State and events available; live CPU/memory requires Datadog / Azure Monitor |
| `get_dlt_health` | `w.pipelines.get()` + `list_updates()` + `list_pipeline_events()` | Full SDK support |
| `get_table_stats` | `w.tables.get()` + `DESCRIBE DETAIL` + `DESCRIBE HISTORY` via SQL | Requires `DATABRICKS_WAREHOUSE_ID` |
| `get_recent_queries` | `w.query_history.list()` | Full SDK support |
| `get_lineage` | `GET /api/2.0/lineage-tracking/table-lineage` | Direct REST — Unity Catalog must be enabled |

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
2. Increase executor memory or enable autoscaling on etl-main-prod.
3. Re-run the aggregate_gold job after OPTIMIZE completes.
4. Schedule weekly OPTIMIZE + VACUUM on silver.events via a Databricks job.
```

---

## Run the Tests

```bash
pytest tests/ -v
```

Tests validate the mock data layer contracts. The same tests run against the real data layer to verify the Databricks API returns the expected schema.

---

## LangSmith Observability

Every `run_query` call is wrapped with `@traceable` and appears as a named root span in LangSmith. All internal LangGraph node transitions and LLM calls are nested under it automatically.

### What you see per trace

```
databricks-observability-query          ← root span (@traceable)
│  Tags: mcp · data-layer:mock          ← filterable in the UI
│  Total tokens: 4,821                  ← auto-aggregated by LangSmith
│
├── LangGraph: agent                    ← round 1 — LLM picks tools
│   └── ChatGroq                        tokens: 1,204
├── LangGraph: tools                    ← MCP tool results
├── LangGraph: agent                    ← round 2 — LLM reasons further
│   └── ChatGroq                        tokens: 2,891
└── LangGraph: agent                    ← final answer
    └── ChatGroq                        tokens:   726
```

### Navigating the UI

| What you want | Where to look |
|---|---|
| Total tokens per query | Runs list → `Total Tokens` column |
| Per-LLM-call token breakdown | Open a trace → click any `ChatGroq` node → **Token Usage** panel |
| Aggregated total for the full run | Open a trace → click the root `databricks-observability-query` node → **Token Usage** panel |
| Filter by data layer | Runs list → filter by tag `data-layer:mock` or `data-layer:databricks` |

### Enabling tracing

Add these to your `.env`:

```
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=your_langsmith_api_key   # smith.langchain.com → Settings → API Keys
LANGCHAIN_PROJECT=databricks-observability
```

LangChain picks these up automatically — no extra code needed beyond what is already in `agent.py`.

---

## Design Decisions

**Why HTTP/SSE transport (Phase 2)?**  
The server runs as a persistent service — multiple agents can connect simultaneously, and the server outlives any single agent run. Phase 1 used stdio (agent spawns server as a subprocess) which is simpler but single-client only.

**Why two-layer authentication?**  
`MCP_API_KEY` controls who can call the MCP server. `DATABRICKS_TOKEN` controls what the MCP server can do in Databricks. These are intentionally separate — the Databricks token never leaves the server.

**Why auto-switch on `DATABRICKS_HOST`?**  
Zero code changes between local demo and production. Set the env var and restart — the server switches data layers automatically.

**Why max 3 tool-call rounds?**  
Prevents runaway tool loops and keeps Groq token costs predictable. Enforced via LangGraph's `recursion_limit=8` (each round = 2 node transitions).

**Why `temperature=0` on the LLM?**  
Observability queries need deterministic, evidence-based answers — not creative ones.

**Production path for auth:**  
Replace API key with OAuth 2.0 (Azure AD / Okta). FastMCP supports JWT validation natively — configure `issuer` and `audience` and it validates tokens against the auth server's public keys automatically. Each agent gets its own service principal with scoped permissions (`read:jobs`, `read:clusters` etc.) and tokens that auto-expire and refresh.
