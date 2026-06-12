"""
Databricks Pipeline Observability MCP Server
Transport: stdio (default for FastMCP)

Run standalone:
    python server.py

The agent connects via stdio — it spawns this process and communicates
over stdin/stdout using the MCP protocol. Do not add print() statements
here; they would corrupt the protocol stream.
"""

from __future__ import annotations

from fastmcp import FastMCP
import mock_data

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
        job_name: Optional filter — one of 'ingest_raw', 'transform_silver',
                  'aggregate_gold', 'ml_feature_eng'. Omit to get all jobs.

    Returns:
        dict with key 'runs': list of run objects each containing job_name,
        state (life_cycle_state, result_state, state_message), timing in ms,
        cluster_instance, and per-task error details.
    """
    return mock_data.get_job_run_status(job_name=job_name)


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

    Known cluster IDs:
        etl_main    → '0601-084523-reef412'
        ml_training → '0601-091200-teal889'
        adhoc_sql   → '0601-072100-sand301'

    Args:
        cluster_id: Optional Databricks cluster ID string to filter to one cluster.
                    Omit to get all clusters.

    Returns:
        dict with key 'clusters': list of cluster objects each containing
        state, node types, autoscale config, and a 'metrics' sub-dict with
        cpu_percent, memory_percent, gc_time_percent, dbu_consumed_last_hour,
        active_tasks, failed_tasks_last_hour.
    """
    return mock_data.get_cluster_health(cluster_id=cluster_id)


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

    Known pipeline IDs:
        bronze_to_silver → 'ple-8f3a2c11-4d90-47b1-a2e5-9b0f3c8d1e4f'
        silver_to_gold   → 'ple-2d7b9e44-1a23-4c56-8f01-3d2e7a9b5c0d'

    Args:
        pipeline_id: Optional DLT pipeline ID string. Omit to get all pipelines.

    Returns:
        dict with key 'pipelines': list of pipeline objects each containing
        state, latest_update (with state and cause), metrics (backlog_bytes,
        backlog_files, processing_rate_mb_per_sec), and a list of recent events
        with level (INFO/WARN/ERROR) and message.
    """
    return mock_data.get_dlt_health(pipeline_id=pipeline_id)


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
    - How fragmented a Delta table is (fragmentation_score 0.0–1.0)

    Known tables (fully-qualified):
        'prod_catalog.silver.events'
        'prod_catalog.gold.kpi_daily'
        'prod_catalog.bronze.raw_clickstream'
        'prod_catalog.silver.user_profiles'

    Args:
        table_name: Optional fully-qualified table name (catalog.schema.table).
                    Omit to get all monitored tables.

    Returns:
        dict with key 'tables': list of table objects each containing
        num_files, size_bytes, partitioning, last_modified, last_optimized,
        last_vacuumed, fragmentation_score, and row_count_estimate.
        fragmentation_score > 0.6 indicates OPTIMIZE is urgently needed.
    """
    return mock_data.get_table_stats(table_name=table_name)


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
            'FAILED' → only queries that errored.
            'SLOW'   → only FINISHED queries that took longer than 30 seconds.
            Omit (None) → return all recent queries.
        limit: Max number of queries to return (default 20).

    Returns:
        dict with key 'queries': list of query objects each containing
        status, query_text, user_name, duration_ms, error_message (if any),
        and a 'metrics' sub-dict with read_bytes, rows_read_count, num_tasks.
    """
    return mock_data.get_recent_queries(status_filter=status_filter, limit=limit)


# ── Tool 6: Table lineage ────────────────────────────────────────────────────

@mcp.tool()
def get_lineage(table_name: str) -> dict:
    """
    Retrieve upstream sources and downstream consumers for a Delta table,
    plus a blast radius score indicating how many pipelines are affected.

    Use this tool AFTER identifying a root cause to quantify impact:
    - Which tables feed the broken table (upstream root causes)?
    - Which downstream tables and dashboards are now stale or broken?
    - What is the blast radius score (1–10) for this table?

    Known tables with lineage data:
        'prod_catalog.bronze.raw_clickstream'
        'prod_catalog.silver.events'
        'prod_catalog.gold.kpi_daily'
        'prod_catalog.silver.user_profiles'

    Args:
        table_name: Fully-qualified table name (catalog.schema.table). Required.

    Returns:
        dict containing:
        - upstreams: list of source tables/streams this table is derived from
        - downstreams: list of tables, jobs, and dashboards that consume this table
        - blast_radius_score: int 1–10 (10 = maximum downstream impact)
        - blast_radius_note: human-readable summary of the impact
    """
    return mock_data.get_lineage(table_name=table_name)


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # stdio transport: the agent process will spawn this script and communicate
    # via stdin/stdout. FastMCP handles the MCP wire protocol automatically.
    mcp.run(transport="stdio")
