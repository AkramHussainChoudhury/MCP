"""
Databricks Pipeline Observability MCP Server

Run as a persistent HTTP service:
    python server.py
Clients connect to http://127.0.0.1:8000/sse

Data layer is selected automatically:
    DATABRICKS_HOST set   → databricks_data.py  (real Databricks SDK calls)
    DATABRICKS_HOST unset → mock_data.py         (local synthetic data)
"""

from __future__ import annotations

import os

from dotenv import load_dotenv
from fastmcp import FastMCP

load_dotenv()


class APIKeyMiddleware:
    """
    Pure ASGI middleware for Bearer token auth.

    BaseHTTPMiddleware buffers the full response body before forwarding it,
    which breaks SSE (the connection stays open and streams events indefinitely).
    A raw ASGI middleware passes scope/receive/send straight through with no
    buffering, so SSE works correctly.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        expected_key = os.environ.get("MCP_API_KEY", "")
        if expected_key:
            headers = {k.lower(): v for k, v in scope.get("headers", [])}
            auth = headers.get(b"authorization", b"").decode("utf-8", errors="replace")
            if auth != f"Bearer {expected_key}":
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [
                        (b"content-type", b"application/json"),
                        (b"content-length", b"24"),
                    ],
                })
                await send({"type": "http.response.body", "body": b'{"error":"Unauthorized"}'})
                return

        await self.app(scope, receive, send)

# Auto-select data layer based on environment
if os.environ.get("DATABRICKS_HOST"):
    import databricks_data as data
else:
    import mock_data as data


# ── Server definition ────────────────────────────────────────────────────────

mcp = FastMCP(
    name="databricks-observability",
    instructions=(
        "You are a Databricks lakehouse health monitor. "
        "Use the available tools to investigate job failures, cluster issues, "
        "DLT pipeline problems, table health, slow queries, and data lineage. "
        "Always check lineage last to quantify blast radius after finding a root cause."
    ),
)


# ── Tool 1: Job run history ──────────────────────────────────────────────────

@mcp.tool()
def get_job_run_status(job_name: str | None = None) -> dict:
    """
    Retrieve recent Databricks job run history including failures and error messages.

    Use this tool when the user asks about:
    - Whether a job succeeded or failed
    - What error a job produced
    - How long a job took
    - Which jobs are currently running

    Args:
        job_name: Optional job name filter. Omit to get runs across all jobs.

    Returns:
        dict with key 'runs': list of run objects each containing job_name,
        state (life_cycle_state, result_state, state_message), timing in ms,
        cluster_instance, and per-task error details.
    """
    return data.get_job_run_status(job_name=job_name)


# ── Tool 2: Cluster health ───────────────────────────────────────────────────

@mcp.tool()
def get_cluster_health(cluster_id: str | None = None) -> dict:
    """
    Retrieve current state and utilisation metrics for Databricks clusters.

    Use this tool when the user asks about:
    - Cluster CPU or memory pressure
    - Whether a cluster is running, terminated, or unhealthy
    - DBU consumption
    - High GC time or task failure rates

    Args:
        cluster_id: Optional Databricks cluster ID string to filter to one cluster.
                    Omit to get all clusters.

    Returns:
        dict with key 'clusters': list of cluster objects each containing
        state, node types, autoscale config, and a 'metrics' sub-dict with
        cpu_percent, memory_percent, gc_time_percent, dbu_consumed_last_hour,
        active_tasks, failed_tasks_last_hour.
    """
    return data.get_cluster_health(cluster_id=cluster_id)


# ── Tool 3: DLT pipeline health ──────────────────────────────────────────────

@mcp.tool()
def get_dlt_health(pipeline_id: str | None = None) -> dict:
    """
    Retrieve Delta Live Tables pipeline state, backlog metrics, and event log.

    Use this tool when the user asks about:
    - Whether a DLT pipeline is healthy or stalled
    - Data backlog size or processing lag
    - Specific DLT flow errors (e.g., write conflicts, schema issues)
    - The most recent pipeline update status

    Args:
        pipeline_id: Optional DLT pipeline ID string. Omit to get all pipelines.

    Returns:
        dict with key 'pipelines': list of pipeline objects each containing
        state, latest_update (with state and cause), metrics (backlog_bytes,
        backlog_files, processing_rate_mb_per_sec), and a list of recent events
        with level (INFO/WARN/ERROR) and message.
    """
    return data.get_dlt_health(pipeline_id=pipeline_id)


# ── Tool 4: Delta table stats ────────────────────────────────────────────────

@mcp.tool()
def get_table_stats(table_name: str | None = None) -> dict:
    """
    Retrieve Delta table health metrics including file count, fragmentation,
    and recency of OPTIMIZE and VACUUM operations.

    Use this tool when the user asks about:
    - Why a query on a table is slow (too many small files)
    - Whether a table needs OPTIMIZE or VACUUM
    - Table size, row count, or partition layout
    - How fragmented a Delta table is (fragmentation_score 0.0-1.0)

    Args:
        table_name: Optional fully-qualified table name (catalog.schema.table).
                    Omit to get all monitored tables.

    Returns:
        dict with key 'tables': list of table objects each containing
        num_files, size_bytes, partitioning, last_modified, last_optimized,
        last_vacuumed, fragmentation_score, and row_count_estimate.
        fragmentation_score > 0.6 indicates OPTIMIZE is urgently needed.
    """
    return data.get_table_stats(table_name=table_name)


# ── Tool 5: Recent SQL queries ───────────────────────────────────────────────

@mcp.tool()
def get_recent_queries(
    status_filter: str | None = None,
    limit: int = 20,
) -> dict:
    """
    Retrieve SQL warehouse query history including failed and slow queries.

    Use this tool when the user asks about:
    - Recent query failures and their error messages
    - Queries that are running slowly or scanning too much data
    - Which users are hitting errors
    - Whether a specific type of query is problematic

    Args:
        status_filter: Optional filter string.
            'FAILED' -> only queries that errored.
            'SLOW'   -> only FINISHED queries that took longer than 30 seconds.
            Omit (None) -> return all recent queries.
        limit: Max number of queries to return (default 20).

    Returns:
        dict with key 'queries': list of query objects each containing
        status, query_text, user_name, duration_ms, error_message (if any),
        and a 'metrics' sub-dict with read_bytes, rows_read_count, num_tasks.
    """
    return data.get_recent_queries(status_filter=status_filter, limit=limit)


# ── Tool 6: Table lineage ────────────────────────────────────────────────────

@mcp.tool()
def get_lineage(table_name: str) -> dict:
    """
    Retrieve upstream sources and downstream consumers for a Delta table,
    plus a blast radius score indicating how many pipelines are affected.

    Use this tool AFTER identifying a root cause to quantify impact:
    - Which tables feed the broken table (upstream root causes)?
    - Which downstream tables and dashboards are now stale or broken?
    - What is the blast radius score (1-10) for this table?

    Args:
        table_name: Fully-qualified table name (catalog.schema.table). Required.

    Returns:
        dict containing:
        - upstreams: list of source tables/streams this table is derived from
        - downstreams: list of tables, jobs, and dashboards that consume this table
        - blast_radius_score: int 1-10 (10 = maximum downstream impact)
        - blast_radius_note: human-readable summary of the impact
    """
    return data.get_lineage(table_name=table_name)


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    # Wrap directly — add_middleware uses Starlette's buffering stack
    # which has the same SSE-breaking behaviour as BaseHTTPMiddleware.
    app = APIKeyMiddleware(mcp.http_app(transport="sse"))

    uvicorn.run(app, host="127.0.0.1", port=8000)
