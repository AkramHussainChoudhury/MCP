"""
Real Databricks data layer — same function signatures as mock_data.py.
server.py imports this automatically when DATABRICKS_HOST is set in the environment.

Authentication (Databricks SDK reads these from env automatically):
    DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
    DATABRICKS_TOKEN=dapi...

Additional env vars:
    DATABRICKS_WAREHOUSE_ID   — SQL warehouse for running DESCRIBE DETAIL / HISTORY
    DATABRICKS_CATALOG        — default catalog for table listing (default: main)
    DATABRICKS_SCHEMA         — default schema for table listing  (default: default)
"""

from __future__ import annotations

import os

import requests
from databricks.sdk import WorkspaceClient

# ── Shared SDK client ────────────────────────────────────────────────────────
# Re-create per call so credentials are always fresh (PAT rotation, OAuth refresh).

def _client() -> WorkspaceClient:
    return WorkspaceClient()


# ── SQL helper ───────────────────────────────────────────────────────────────

def _run_sql(sql: str) -> list[dict]:
    """
    Execute SQL via the Statement Execution API and return rows as list of dicts.
    Requires DATABRICKS_WAREHOUSE_ID in environment.
    """
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        raise EnvironmentError(
            "DATABRICKS_WAREHOUSE_ID is not set — needed for table stats queries"
        )
    w = _client()
    response = w.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        wait_timeout="30s",
    )
    if not response.result or not response.result.data_array:
        return []
    cols = [c.name for c in response.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in response.result.data_array]


# ── 1. Job run history ───────────────────────────────────────────────────────

def get_job_run_status(job_name: str | None = None) -> dict:
    """
    Real API: GET /api/2.1/jobs/runs/list
    If job_name is given, resolves to job_id via /api/2.1/jobs/list first.
    """
    w = _client()

    job_id = None
    if job_name:
        matched = list(w.jobs.list(name=job_name))
        if not matched:
            return {"runs": [], "has_more": False}
        job_id = matched[0].job_id

    runs_iter = w.jobs.list_runs(
        job_id=job_id,
        limit=10,
        expand_tasks=True,
    )

    runs = []
    for run in runs_iter:
        state = run.state
        tasks = [
            {
                "task_key": t.task_key,
                "state": {
                    "life_cycle_state": t.state.life_cycle_state.value
                    if t.state and t.state.life_cycle_state else None,
                    "result_state": t.state.result_state.value
                    if t.state and t.state.result_state else None,
                },
                "error_message": t.state.state_message if t.state else None,
            }
            for t in (run.tasks or [])
        ]

        runs.append({
            "run_id": run.run_id,
            "job_id": run.job_id,
            "job_name": run.run_name or "",
            "run_name": run.run_name or "",
            "state": {
                "life_cycle_state": state.life_cycle_state.value
                if state and state.life_cycle_state else None,
                "result_state": state.result_state.value
                if state and state.result_state else None,
                "state_message": state.state_message or "" if state else "",
            },
            "start_time_ms": run.start_time,
            "end_time_ms": run.end_time,
            "execution_duration_ms": run.execution_duration,
            "cluster_instance": {
                "cluster_id": run.cluster_instance.cluster_id
                if run.cluster_instance else None
            },
            "attempt_number": run.attempt_number or 0,
            "tasks": tasks,
        })

    return {"runs": runs, "has_more": False}


# ── 2. Cluster health ────────────────────────────────────────────────────────

def get_cluster_health(cluster_id: str | None = None) -> dict:
    """
    Real API: GET /api/2.0/clusters/list  or  /api/2.0/clusters/get

    NOTE: Databricks REST API does not expose live CPU / memory / GC metrics.
    Those require an external monitoring integration (Datadog, Azure Monitor,
    AWS CloudWatch, or Ganglia if still enabled on your workspace).
    We return cluster state, config, and recent events as health proxies.
    """
    w = _client()

    raw_clusters = (
        [w.clusters.get(cluster_id)] if cluster_id
        else list(w.clusters.list())
    )

    clusters = []
    for c in raw_clusters:
        # Cluster events give health signals (OOM, node lost, preemption, etc.)
        try:
            events = list(w.clusters.events(cluster_id=c.cluster_id, limit=5))
            recent_events = [
                {
                    "timestamp": e.timestamp,
                    "type": e.type.value if e.type else None,
                    "details": str(e.details) if e.details else None,
                }
                for e in events
            ]
        except Exception:
            recent_events = []

        clusters.append({
            "cluster_id": c.cluster_id,
            "cluster_name": c.cluster_name,
            "state": c.state.value if c.state else None,
            "state_message": c.state_message or "",
            "driver_node_type_id": c.driver_node_type_id,
            "node_type_id": c.node_type_id,
            "num_workers": c.num_workers,
            "autoscale": {
                "min_workers": c.autoscale.min_workers,
                "max_workers": c.autoscale.max_workers,
            } if c.autoscale else None,
            "spark_version": c.spark_version,
            "last_state_loss_time": c.last_state_loss_time,
            "metrics": {
                # Live metrics not available via Databricks REST API.
                # Integrate with Datadog / Azure Monitor for cpu_percent,
                # memory_percent, gc_time_percent etc.
                "cpu_percent": None,
                "memory_used_mb": None,
                "memory_total_mb": None,
                "memory_percent": None,
                "active_tasks": None,
                "failed_tasks_last_hour": None,
                "dbu_consumed_last_hour": None,
                "gc_time_percent": None,
                # Cluster events are the available health signal from the API
                "recent_events": recent_events,
            },
        })

    return {"clusters": clusters}


# ── 3. DLT pipeline health ───────────────────────────────────────────────────

def get_dlt_health(pipeline_id: str | None = None) -> dict:
    """
    Real API:
        GET /api/2.0/pipelines/{id}
        GET /api/2.0/pipelines/{id}/updates
        GET /api/2.0/pipelines/{id}/events
    """
    w = _client()

    raw_pipelines = (
        [w.pipelines.get(pipeline_id)] if pipeline_id
        else list(w.pipelines.list_pipelines())
    )

    pipelines = []
    for p in raw_pipelines:
        pid = p.pipeline_id

        # Latest update
        try:
            updates = list(w.pipelines.list_updates(pipeline_id=pid, max_results=1))
            latest = updates[0] if updates else None
        except Exception:
            latest = None

        # Recent events (last 10)
        try:
            raw_events = list(w.pipelines.list_pipeline_events(
                pipeline_id=pid, max_results=10
            ))
            events = [
                {
                    "id": e.id,
                    "timestamp": e.timestamp,
                    "level": e.level.value if e.level else None,
                    "message": e.message,
                    "origin": {
                        "pipeline_id": e.origin.pipeline_id if e.origin else None,
                        "flow_name": e.origin.flow_name if e.origin else None,
                        "dataset_name": e.origin.dataset_name if e.origin else None,
                    } if e.origin else {},
                }
                for e in raw_events
            ]
        except Exception:
            events = []

        pipelines.append({
            "pipeline_id": pid,
            "name": p.name,
            "state": p.state.value if p.state else None,
            "catalog": p.catalog,
            "target_schema": p.target,
            "continuous": p.continuous or False,
            "latest_update": {
                "update_id": latest.update_id if latest else None,
                "state": latest.state.value if latest and latest.state else None,
                "cause": latest.cause if latest else None,
                "full_refresh": latest.full_refresh if latest else None,
                "creation_time": latest.creation_time if latest else None,
                "update_type": latest.update_type.value
                if latest and latest.update_type else None,
            } if latest else {},
            # DLT backlog metrics are not exposed via the REST API directly.
            # They appear in pipeline events — parse events for WARN/backlog messages.
            "metrics": {
                "backlog_bytes": None,
                "backlog_files": None,
                "num_queued_updates": None,
                "processing_rate_mb_per_sec": None,
            },
            "events": events,
        })

    return {"pipelines": pipelines}


# ── 4. Delta table stats ─────────────────────────────────────────────────────

# Files below this size indicate fragmentation (Databricks recommendation: ~128 MB)
_TARGET_FILE_SIZE_BYTES = 128 * 1024 * 1024

def get_table_stats(table_name: str | None = None) -> dict:
    """
    Real API:
        GET /api/2.1/unity-catalog/tables/{full_name}  — basic metadata
        DESCRIBE DETAIL <table>                         — Delta file stats
        DESCRIBE HISTORY <table> LIMIT 50              — last OPTIMIZE / VACUUM

    Requires DATABRICKS_WAREHOUSE_ID for the SQL calls.
    """
    w = _client()

    if table_name:
        fq_names = [table_name]
    else:
        catalog = os.environ.get("DATABRICKS_CATALOG", "main")
        schema  = os.environ.get("DATABRICKS_SCHEMA", "default")
        fq_names = [
            f"{t.catalog_name}.{t.schema_name}.{t.name}"
            for t in w.tables.list(catalog_name=catalog, schema_name=schema)
            if t.data_source_format and t.data_source_format.value == "DELTA"
        ]

    tables = []
    for fqn in fq_names:
        # Basic Unity Catalog metadata
        try:
            meta = w.tables.get(fqn)
        except Exception:
            continue

        # DESCRIBE DETAIL — Delta file count and size
        detail: dict = {}
        try:
            rows = _run_sql(f"DESCRIBE DETAIL `{fqn}`")
            if rows:
                detail = rows[0]
        except Exception:
            pass

        # DESCRIBE HISTORY — last OPTIMIZE and VACUUM timestamps
        last_optimized = None
        last_vacuumed  = None
        try:
            history = _run_sql(f"DESCRIBE HISTORY `{fqn}` LIMIT 50")
            for row in history:
                op = row.get("operation", "")
                ts = row.get("timestamp")
                if op == "OPTIMIZE" and not last_optimized:
                    last_optimized = ts
                if op in ("VACUUM END", "VACUUM") and not last_vacuumed:
                    last_vacuumed = ts
        except Exception:
            pass

        num_files  = int(detail.get("numFiles")  or 0)
        size_bytes = int(detail.get("sizeInBytes") or 0)

        # Fragmentation: ratio of actual files to ideal file count at 128 MB each
        ideal = max(1, size_bytes / _TARGET_FILE_SIZE_BYTES)
        ratio = num_files / ideal
        # ratio=1 → perfectly compacted (score 0.0); ratio≥10 → severely fragmented (score 1.0)
        fragmentation_score = round(min(1.0, max(0.0, (ratio - 1) / 9)), 2)

        tables.append({
            "table_name": fqn,
            "catalog": meta.catalog_name,
            "schema": meta.schema_name,
            "table_type": meta.table_type.value if meta.table_type else None,
            "data_source_format": meta.data_source_format.value
            if meta.data_source_format else None,
            "location": meta.storage_location,
            "partitioning": meta.partition_column_names or [],
            "num_files": num_files,
            "size_bytes": size_bytes,
            "last_modified": detail.get("lastModified"),
            "last_optimized": last_optimized,
            "last_vacuumed": last_vacuumed,
            "fragmentation_score": fragmentation_score,
            "delta_log_entries": None,
            "row_count_estimate": None,
        })

    return {"tables": tables}


# ── 5. Recent SQL queries ────────────────────────────────────────────────────

def get_recent_queries(
    status_filter: str | None = None,
    limit: int = 20,
) -> dict:
    """
    Real API: GET /api/2.0/sql/history/queries
    'SLOW' is a synthetic label: FINISHED queries where duration > 30 seconds.
    """
    w = _client()

    # Fetch extra to allow for client-side filtering
    raw = list(w.query_history.list(max_results=limit * 3, include_metrics=True))

    queries = []
    for q in raw:
        status      = q.status.value if q.status else None
        duration_ms = q.duration or 0

        if status_filter == "FAILED" and status != "FAILED":
            continue
        if status_filter == "SLOW" and not (status == "FINISHED" and duration_ms > 30_000):
            continue

        m = q.metrics
        queries.append({
            "query_id": q.query_id,
            "status": status,
            "query_text": q.query_text,
            "user_name": q.user_name,
            "warehouse_id": q.warehouse_id,
            "start_time_ms": q.query_start_time_ms,
            "end_time_ms": q.query_end_time_ms,
            "duration_ms": duration_ms,
            "rows_produced": q.rows_produced,
            "error_message": q.error_message,
            "metrics": {
                "read_bytes": m.read_bytes if m else None,
                "result_fetch_time_ms": m.result_fetch_time_ms if m else None,
                "compilation_time_ms": m.compilation_time_ms if m else None,
                "execution_time_ms": m.execution_time_ms if m else None,
                "rows_read_count": m.rows_read_count if m else None,
                "num_tasks": None,
            },
        })

        if len(queries) >= limit:
            break

    return {"queries": queries, "total_count": len(queries)}


# ── 6. Table lineage ─────────────────────────────────────────────────────────

def get_lineage(table_name: str) -> dict:
    """
    Real API: GET /api/2.0/lineage-tracking/table-lineage
    Uses direct HTTP — Unity Catalog lineage is not fully covered by the SDK.
    Requires Unity Catalog to be enabled on the workspace.
    """
    host  = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")

    try:
        response = requests.get(
            f"{host}/api/2.0/lineage-tracking/table-lineage",
            headers={"Authorization": f"Bearer {token}"},
            params={"table_name": table_name},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()

        def _normalise(entry: dict, relationship: str) -> dict:
            return {
                "table_name": f"{entry.get('catalog_name','')}.{entry.get('schema_name','')}.{entry.get('name','')}",
                "catalog": entry.get("catalog_name"),
                "schema": entry.get("schema_name"),
                "table_type": entry.get("table_type"),
                "relationship": relationship,
                "job_name": None,
            }

        upstreams   = [_normalise(u, "DERIVED_FROM") for u in data.get("upstreams",   [])]
        downstreams = [_normalise(d, "DERIVED_FROM") for d in data.get("downstreams", [])]

        score = min(10, len(downstreams))
        note  = (
            f"{len(downstreams)} downstream table(s) depend on this table. "
            f"Upstream sources: {len(upstreams)}."
        )

        return {
            "table_name": table_name,
            "upstreams": upstreams,
            "downstreams": downstreams,
            "blast_radius_score": score,
            "blast_radius_note": note,
        }

    except Exception as exc:
        return {
            "table_name": table_name,
            "upstreams": [],
            "downstreams": [],
            "blast_radius_score": 0,
            "blast_radius_note": f"Lineage unavailable: {exc}",
        }
