"""
Mock data layer — mirrors Databricks REST API response schemas.
Each function returns a dict identical in shape to the real API response.
To go live: replace each function body with the corresponding SDK/REST call.

Real API refs:
  Jobs:     GET /api/2.1/jobs/runs/list
  Clusters: GET /api/2.0/clusters/list  + custom metrics endpoint
  DLT:      GET /api/2.0/pipelines/{id}/events
  Tables:   GET /api/2.1/unity-catalog/tables/{full_name}  (+ DESCRIBE DETAIL)
  Queries:  GET /api/2.0/sql/history/queries
  Lineage:  GET /api/2.0/lineage-tracking/table-lineage
"""

from __future__ import annotations


# ─────────────────────────────────────────────────────────────
# Shared reference IDs  (realistic Databricks format)
# ─────────────────────────────────────────────────────────────

CLUSTER_IDS = {
    "etl_main":   "0601-084523-reef412",
    "ml_training": "0601-091200-teal889",
    "adhoc_sql":  "0601-072100-sand301",
}

JOB_IDS = {
    "ingest_raw":       1001,
    "transform_silver": 1002,
    "aggregate_gold":   1003,
    "ml_feature_eng":   1004,
}

DLT_PIPELINE_IDS = {
    "bronze_to_silver": "ple-8f3a2c11-4d90-47b1-a2e5-9b0f3c8d1e4f",
    "silver_to_gold":   "ple-2d7b9e44-1a23-4c56-8f01-3d2e7a9b5c0d",
}

WAREHOUSE_ID = "abc123def456"


# ─────────────────────────────────────────────────────────────
# 1. Job run history
# ─────────────────────────────────────────────────────────────

def get_job_run_status(job_name: str | None = None) -> dict:
    """
    Returns the 10 most recent runs across monitored jobs.
    Mirrors: GET /api/2.1/jobs/runs/list?limit=10&expand_tasks=true
    """
    runs = [
        # --- aggregate_gold  (latest run — FAILED) ---
        {
            "run_id": 88421,
            "job_id": JOB_IDS["aggregate_gold"],
            "job_name": "aggregate_gold",
            "run_name": "aggregate_gold_20260609_0300",
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": (
                    "java.lang.OutOfMemoryError: GC overhead limit exceeded "
                    "in task stage 14.0 (TID 892)"
                ),
            },
            "start_time_ms": 1749430200000,   # 2026-06-09 03:30 UTC
            "end_time_ms":   1749430980000,   # 13 min later
            "execution_duration_ms": 780000,
            "cluster_instance": {"cluster_id": CLUSTER_IDS["etl_main"]},
            "attempt_number": 1,
            "tasks": [
                {
                    "task_key": "compute_kpis",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                    },
                    "error_message": "OOM in shuffle stage — executor heap exhausted",
                }
            ],
        },
        # --- aggregate_gold  (previous run — SUCCESS) ---
        {
            "run_id": 88387,
            "job_id": JOB_IDS["aggregate_gold"],
            "job_name": "aggregate_gold",
            "run_name": "aggregate_gold_20260608_0300",
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
            },
            "start_time_ms": 1749343800000,
            "end_time_ms":   1749344640000,
            "execution_duration_ms": 840000,
            "cluster_instance": {"cluster_id": CLUSTER_IDS["etl_main"]},
            "attempt_number": 0,
            "tasks": [],
        },
        # --- transform_silver (FAILED — upstream schema drift) ---
        {
            "run_id": 88410,
            "job_id": JOB_IDS["transform_silver"],
            "job_name": "transform_silver",
            "run_name": "transform_silver_20260609_0200",
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": (
                    "AnalysisException: Cannot resolve column 'event_type' "
                    "in table raw.clickstream — schema drift detected"
                ),
            },
            "start_time_ms": 1749426600000,
            "end_time_ms":   1749427020000,
            "execution_duration_ms": 420000,
            "cluster_instance": {"cluster_id": CLUSTER_IDS["etl_main"]},
            "attempt_number": 0,
            "tasks": [
                {
                    "task_key": "deduplicate",
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                    },
                    "error_message": "Schema evolution not enabled; column 'event_type' missing",
                }
            ],
        },
        # --- ingest_raw (SUCCESS) ---
        {
            "run_id": 88400,
            "job_id": JOB_IDS["ingest_raw"],
            "job_name": "ingest_raw",
            "run_name": "ingest_raw_20260609_0100",
            "state": {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "",
            },
            "start_time_ms": 1749423000000,
            "end_time_ms":   1749423480000,
            "execution_duration_ms": 480000,
            "cluster_instance": {"cluster_id": CLUSTER_IDS["etl_main"]},
            "attempt_number": 0,
            "tasks": [],
        },
        # --- ml_feature_eng (RUNNING) ---
        {
            "run_id": 88430,
            "job_id": JOB_IDS["ml_feature_eng"],
            "job_name": "ml_feature_eng",
            "run_name": "ml_feature_eng_20260609_0400",
            "state": {
                "life_cycle_state": "RUNNING",
                "result_state": None,
                "state_message": "Task 'feature_join' in progress",
            },
            "start_time_ms": 1749434400000,
            "end_time_ms":   None,
            "execution_duration_ms": None,
            "cluster_instance": {"cluster_id": CLUSTER_IDS["ml_training"]},
            "attempt_number": 0,
            "tasks": [],
        },
    ]

    if job_name:
        runs = [r for r in runs if r["job_name"] == job_name]

    return {"runs": runs, "has_more": False}


# ─────────────────────────────────────────────────────────────
# 2. Cluster health
# ─────────────────────────────────────────────────────────────

def get_cluster_health(cluster_id: str | None = None) -> dict:
    """
    Returns cluster state + utilisation metrics for all monitored clusters.
    Mirrors: GET /api/2.0/clusters/list  +  metrics sidecar response.
    """
    clusters = [
        {
            "cluster_id": CLUSTER_IDS["etl_main"],
            "cluster_name": "etl-main-prod",
            "state": "RUNNING",
            "state_message": "",
            "driver_node_type_id": "i3.2xlarge",
            "node_type_id": "i3.xlarge",
            "num_workers": 8,
            "autoscale": {"min_workers": 4, "max_workers": 16},
            "spark_version": "14.3.x-scala2.12",
            "dbu_per_hour": 6.4,
            "metrics": {
                "cpu_percent": 91.3,          # HIGH — root cause of OOM
                "memory_used_mb": 61_440,
                "memory_total_mb": 65_536,
                "memory_percent": 93.7,       # HIGH
                "disk_used_gb": 820,
                "disk_total_gb": 960,
                "active_tasks": 48,
                "failed_tasks_last_hour": 14,
                "dbu_consumed_last_hour": 5.9,
                "gc_time_percent": 18.4,      # HIGH — correlates with OOM
            },
            "last_state_loss_time": 1749430200000,
        },
        {
            "cluster_id": CLUSTER_IDS["ml_training"],
            "cluster_name": "ml-training-dev",
            "state": "RUNNING",
            "state_message": "",
            "driver_node_type_id": "p3.2xlarge",
            "node_type_id": "p3.xlarge",
            "num_workers": 4,
            "autoscale": None,
            "spark_version": "14.3.x-gpu-ml-scala2.12",
            "dbu_per_hour": 8.0,
            "metrics": {
                "cpu_percent": 44.1,
                "memory_used_mb": 28_672,
                "memory_total_mb": 61_440,
                "memory_percent": 46.7,
                "disk_used_gb": 210,
                "disk_total_gb": 480,
                "active_tasks": 6,
                "failed_tasks_last_hour": 0,
                "dbu_consumed_last_hour": 7.2,
                "gc_time_percent": 2.1,
            },
            "last_state_loss_time": None,
        },
        {
            "cluster_id": CLUSTER_IDS["adhoc_sql"],
            "cluster_name": "adhoc-sql-shared",
            "state": "TERMINATED",
            "state_message": "Cluster terminated after 120 min idle",
            "driver_node_type_id": "m5.xlarge",
            "node_type_id": "m5.large",
            "num_workers": 0,
            "autoscale": None,
            "spark_version": "14.3.x-scala2.12",
            "dbu_per_hour": 0.0,
            "metrics": {
                "cpu_percent": 0.0,
                "memory_used_mb": 0,
                "memory_total_mb": 32_768,
                "memory_percent": 0.0,
                "disk_used_gb": 0,
                "disk_total_gb": 240,
                "active_tasks": 0,
                "failed_tasks_last_hour": 0,
                "dbu_consumed_last_hour": 0.0,
                "gc_time_percent": 0.0,
            },
            "last_state_loss_time": None,
        },
    ]

    if cluster_id:
        clusters = [c for c in clusters if c["cluster_id"] == cluster_id]

    return {"clusters": clusters}


# ─────────────────────────────────────────────────────────────
# 3. DLT pipeline health
# ─────────────────────────────────────────────────────────────

def get_dlt_health(pipeline_id: str | None = None) -> dict:
    """
    Returns DLT pipeline state, latest update, and last 10 events.
    Mirrors: GET /api/2.0/pipelines/{id}  +  /api/2.0/pipelines/{id}/events
    """
    pipelines = [
        {
            "pipeline_id": DLT_PIPELINE_IDS["bronze_to_silver"],
            "name": "bronze_to_silver",
            "state": "RUNNING",
            "catalog": "prod_catalog",
            "target_schema": "silver",
            "continuous": True,
            "latest_update": {
                "update_id": "upd-b2s-20260609-001",
                "state": "WAITING_FOR_RESOURCES",
                "cause": "Cluster autoscaling — waiting for 2 worker nodes",
                "full_refresh": False,
                "creation_time": 1749434100000,
                "update_type": "TRIGGERED",
            },
            "metrics": {
                "backlog_bytes": 4_294_967_296,   # 4 GB backlog — notable lag
                "backlog_files": 18_432,
                "num_queued_updates": 3,
                "processing_rate_mb_per_sec": 0.0,  # stalled
            },
            "events": [
                {
                    "id": "evt-001",
                    "timestamp": "2026-06-09T04:15:00Z",
                    "level": "WARN",
                    "message": "Backlog exceeded 4 GB — processing stalled awaiting cluster resources",
                    "origin": {"pipeline_id": DLT_PIPELINE_IDS["bronze_to_silver"]},
                },
                {
                    "id": "evt-002",
                    "timestamp": "2026-06-09T04:00:00Z",
                    "level": "INFO",
                    "message": "Update upd-b2s-20260609-001 started",
                    "origin": {"pipeline_id": DLT_PIPELINE_IDS["bronze_to_silver"]},
                },
                {
                    "id": "evt-003",
                    "timestamp": "2026-06-09T03:45:00Z",
                    "level": "ERROR",
                    "message": (
                        "Flow 'raw_events_to_silver' failed: "
                        "DELTA_CONCURRENT_WRITE — concurrent write conflict on silver.events"
                    ),
                    "origin": {
                        "pipeline_id": DLT_PIPELINE_IDS["bronze_to_silver"],
                        "flow_name": "raw_events_to_silver",
                        "dataset_name": "silver.events",
                    },
                },
            ],
        },
        {
            "pipeline_id": DLT_PIPELINE_IDS["silver_to_gold"],
            "name": "silver_to_gold",
            "state": "IDLE",
            "catalog": "prod_catalog",
            "target_schema": "gold",
            "continuous": False,
            "latest_update": {
                "update_id": "upd-s2g-20260608-012",
                "state": "COMPLETED",
                "cause": None,
                "full_refresh": False,
                "creation_time": 1749340800000,
                "update_type": "TRIGGERED",
            },
            "metrics": {
                "backlog_bytes": 0,
                "backlog_files": 0,
                "num_queued_updates": 0,
                "processing_rate_mb_per_sec": 0.0,
            },
            "events": [
                {
                    "id": "evt-010",
                    "timestamp": "2026-06-08T04:00:00Z",
                    "level": "INFO",
                    "message": "Update upd-s2g-20260608-012 completed successfully",
                    "origin": {"pipeline_id": DLT_PIPELINE_IDS["silver_to_gold"]},
                }
            ],
        },
    ]

    if pipeline_id:
        pipelines = [p for p in pipelines if p["pipeline_id"] == pipeline_id]

    return {"pipelines": pipelines}


# ─────────────────────────────────────────────────────────────
# 4. Delta table stats
# ─────────────────────────────────────────────────────────────

def get_table_stats(table_name: str | None = None) -> dict:
    """
    Returns Delta table metadata including fragmentation signals.
    Mirrors: DESCRIBE DETAIL <table>  +  Unity Catalog table info.
    fragmentation_score: 0.0 (healthy) → 1.0 (severely fragmented)
    """
    tables = [
        {
            "table_name": "prod_catalog.silver.events",
            "catalog": "prod_catalog",
            "schema": "silver",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "location": "dbfs:/mnt/prod/silver/events",
            "storage_properties": {"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"},
            "num_files": 28_470,            # HIGH — should be <5k after OPTIMIZE
            "size_bytes": 107_374_182_400,  # 100 GB
            "partitioning": ["event_date"],
            "last_modified": "2026-06-09T04:18:00Z",
            "last_optimized": "2026-05-10T02:00:00Z",   # 30 days ago — stale
            "last_vacuumed": "2026-05-01T02:00:00Z",    # 39 days ago — stale
            "fragmentation_score": 0.81,                 # HIGH — needs OPTIMIZE
            "delta_log_entries": 9_412,
            "row_count_estimate": 4_200_000_000,
        },
        {
            "table_name": "prod_catalog.gold.kpi_daily",
            "catalog": "prod_catalog",
            "schema": "gold",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "location": "dbfs:/mnt/prod/gold/kpi_daily",
            "storage_properties": {"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"},
            "num_files": 312,
            "size_bytes": 524_288_000,   # 500 MB
            "partitioning": ["report_date"],
            "last_modified": "2026-06-09T03:55:00Z",
            "last_optimized": "2026-06-08T02:00:00Z",
            "last_vacuumed": "2026-06-07T02:00:00Z",
            "fragmentation_score": 0.12,
            "delta_log_entries": 448,
            "row_count_estimate": 1_800_000,
        },
        {
            "table_name": "prod_catalog.bronze.raw_clickstream",
            "catalog": "prod_catalog",
            "schema": "bronze",
            "table_type": "EXTERNAL",
            "data_source_format": "DELTA",
            "location": "dbfs:/mnt/prod/bronze/raw_clickstream",
            "storage_properties": {"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"},
            "num_files": 4_102,
            "size_bytes": 214_748_364_800,  # 200 GB
            "partitioning": ["ingest_date", "source_system"],
            "last_modified": "2026-06-09T04:20:00Z",
            "last_optimized": "2026-06-06T01:00:00Z",
            "last_vacuumed": "2026-06-05T01:00:00Z",
            "fragmentation_score": 0.34,
            "delta_log_entries": 1_204,
            "row_count_estimate": 18_000_000_000,
        },
        {
            "table_name": "prod_catalog.silver.user_profiles",
            "catalog": "prod_catalog",
            "schema": "silver",
            "table_type": "MANAGED",
            "data_source_format": "DELTA",
            "location": "dbfs:/mnt/prod/silver/user_profiles",
            "storage_properties": {"delta.minReaderVersion": "1", "delta.minWriterVersion": "2"},
            "num_files": 88,
            "size_bytes": 10_737_418_240,   # 10 GB
            "partitioning": [],
            "last_modified": "2026-06-09T02:00:00Z",
            "last_optimized": "2026-06-09T01:00:00Z",
            "last_vacuumed": "2026-06-09T01:05:00Z",
            "fragmentation_score": 0.05,
            "delta_log_entries": 122,
            "row_count_estimate": 45_000_000,
        },
    ]

    if table_name:
        tables = [t for t in tables if t["table_name"] == table_name]

    return {"tables": tables}


# ─────────────────────────────────────────────────────────────
# 5. Recent SQL queries
# ─────────────────────────────────────────────────────────────

def get_recent_queries(
    status_filter: str | None = None,   # "FAILED" | "SLOW" | None (all)
    limit: int = 20,
) -> dict:
    """
    Returns SQL warehouse query history, newest first.
    Mirrors: GET /api/2.0/sql/history/queries?max_results=N
    'SLOW' is a synthetic label: any query where duration_ms > 30_000.
    """
    queries = [
        # --- FAILED: OOM on massive join ---
        {
            "query_id": "qry-f1a2b3c4",
            "status": "FAILED",
            "query_text": (
                "SELECT u.user_id, COUNT(e.event_id) AS event_count "
                "FROM prod_catalog.silver.events e "
                "JOIN prod_catalog.silver.user_profiles u ON e.user_id = u.user_id "
                "WHERE e.event_date >= '2026-01-01' "
                "GROUP BY u.user_id "
                "ORDER BY event_count DESC"
            ),
            "user_name": "analyst@company.com",
            "warehouse_id": WAREHOUSE_ID,
            "start_time_ms": 1749430500000,
            "end_time_ms":   1749430740000,
            "duration_ms": 240_000,
            "rows_produced": 0,
            "error_message": (
                "SparkException: Job aborted due to stage failure: "
                "Task 0 in stage 8.0 failed — executor OOM "
                "(see cluster 0601-084523-reef412 GC logs)"
            ),
            "metrics": {
                "read_bytes": 96_636_764_160,   # read almost entire events table
                "result_fetch_time_ms": 0,
                "compilation_time_ms": 1_200,
                "execution_time_ms": 238_800,
                "rows_read_count": 0,
                "num_tasks": 48,
            },
        },
        # --- SLOW: full scan, no partition pruning ---
        {
            "query_id": "qry-a9b8c7d6",
            "status": "FINISHED",
            "query_text": (
                "SELECT event_type, COUNT(*) "
                "FROM prod_catalog.silver.events "
                "GROUP BY event_type"
            ),
            "user_name": "reporting@company.com",
            "warehouse_id": WAREHOUSE_ID,
            "start_time_ms": 1749428400000,
            "end_time_ms":   1749428520000,
            "duration_ms": 120_000,    # 2 min — SLOW
            "rows_produced": 42,
            "error_message": None,
            "metrics": {
                "read_bytes": 107_374_182_400,  # full table scan
                "result_fetch_time_ms": 80,
                "compilation_time_ms": 950,
                "execution_time_ms": 119_000,
                "rows_read_count": 4_200_000_000,
                "num_tasks": 894,               # HIGH — 28k files × small tasks
            },
        },
        # --- FAILED: schema drift (missing column) ---
        {
            "query_id": "qry-e5f6g7h8",
            "status": "FAILED",
            "query_text": (
                "SELECT user_id, event_type, session_id "
                "FROM prod_catalog.bronze.raw_clickstream "
                "WHERE ingest_date = '2026-06-09' LIMIT 100"
            ),
            "user_name": "data_eng@company.com",
            "warehouse_id": WAREHOUSE_ID,
            "start_time_ms": 1749426000000,
            "end_time_ms":   1749426012000,
            "duration_ms": 12_000,
            "rows_produced": 0,
            "error_message": (
                "AnalysisException: Column 'event_type' does not exist. "
                "Did you mean: ['evt_type', 'event_category']? "
                "Source schema was modified without migration."
            ),
            "metrics": {
                "read_bytes": 0,
                "result_fetch_time_ms": 0,
                "compilation_time_ms": 12_000,
                "execution_time_ms": 0,
                "rows_read_count": 0,
                "num_tasks": 0,
            },
        },
        # --- SUCCESS: fast, healthy query ---
        {
            "query_id": "qry-i1j2k3l4",
            "status": "FINISHED",
            "query_text": (
                "SELECT report_date, SUM(revenue) AS total_revenue "
                "FROM prod_catalog.gold.kpi_daily "
                "WHERE report_date >= '2026-06-01' "
                "GROUP BY report_date ORDER BY report_date"
            ),
            "user_name": "analyst@company.com",
            "warehouse_id": WAREHOUSE_ID,
            "start_time_ms": 1749427200000,
            "end_time_ms":   1749427203800,
            "duration_ms": 3_800,
            "rows_produced": 8,
            "error_message": None,
            "metrics": {
                "read_bytes": 2_097_152,
                "result_fetch_time_ms": 40,
                "compilation_time_ms": 620,
                "execution_time_ms": 3_100,
                "rows_read_count": 45_000,
                "num_tasks": 4,
            },
        },
        # --- FAILED: permission denied ---
        {
            "query_id": "qry-m5n6o7p8",
            "status": "FAILED",
            "query_text": "SELECT * FROM prod_catalog.silver.user_profiles LIMIT 10",
            "user_name": "intern@company.com",
            "warehouse_id": WAREHOUSE_ID,
            "start_time_ms": 1749425000000,
            "end_time_ms":   1749425001200,
            "duration_ms": 1_200,
            "rows_produced": 0,
            "error_message": (
                "PERMISSION_DENIED: User intern@company.com does not have "
                "SELECT privilege on table prod_catalog.silver.user_profiles"
            ),
            "metrics": {
                "read_bytes": 0,
                "result_fetch_time_ms": 0,
                "compilation_time_ms": 1_200,
                "execution_time_ms": 0,
                "rows_read_count": 0,
                "num_tasks": 0,
            },
        },
    ]

    if status_filter == "FAILED":
        queries = [q for q in queries if q["status"] == "FAILED"]
    elif status_filter == "SLOW":
        # synthetic label: FINISHED queries longer than 30 seconds
        queries = [q for q in queries if q["status"] == "FINISHED" and q["duration_ms"] > 30_000]

    return {"queries": queries[:limit], "total_count": len(queries)}


# ─────────────────────────────────────────────────────────────
# 6. Table lineage (blast radius)
# ─────────────────────────────────────────────────────────────

def get_lineage(table_name: str) -> dict:
    """
    Returns upstream sources and downstream consumers for a given table.
    Mirrors: GET /api/2.0/lineage-tracking/table-lineage?table_name=<fqn>
    Use blast_radius_score (1–10) to quantify downstream impact.
    """
    lineage_map = {
        "prod_catalog.bronze.raw_clickstream": {
            "table_name": "prod_catalog.bronze.raw_clickstream",
            "upstreams": [
                {
                    "table_name": "external.kafka.clickstream_topic",
                    "catalog": "external",
                    "schema": "kafka",
                    "table_type": "STREAMING_SOURCE",
                    "relationship": "WRITTEN_BY",
                }
            ],
            "downstreams": [
                {
                    "table_name": "prod_catalog.silver.events",
                    "catalog": "prod_catalog",
                    "schema": "silver",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "transform_silver",
                },
                {
                    "table_name": "prod_catalog.silver.user_profiles",
                    "catalog": "prod_catalog",
                    "schema": "silver",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "transform_silver",
                },
            ],
            "blast_radius_score": 9,
            "blast_radius_note": (
                "Source for all silver tables; silver feeds all gold KPI tables. "
                "Schema change here cascades to 6 downstream tables and 3 DLT flows."
            ),
        },
        "prod_catalog.silver.events": {
            "table_name": "prod_catalog.silver.events",
            "upstreams": [
                {
                    "table_name": "prod_catalog.bronze.raw_clickstream",
                    "catalog": "prod_catalog",
                    "schema": "bronze",
                    "table_type": "EXTERNAL",
                    "relationship": "DERIVED_FROM",
                    "job_name": "transform_silver",
                }
            ],
            "downstreams": [
                {
                    "table_name": "prod_catalog.gold.kpi_daily",
                    "catalog": "prod_catalog",
                    "schema": "gold",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "aggregate_gold",
                },
                {
                    "table_name": "prod_catalog.gold.funnel_analysis",
                    "catalog": "prod_catalog",
                    "schema": "gold",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "aggregate_gold",
                },
                {
                    "table_name": "prod_catalog.gold.retention_cohorts",
                    "catalog": "prod_catalog",
                    "schema": "gold",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "aggregate_gold",
                },
                {
                    "table_name": "ml_catalog.features.user_event_features",
                    "catalog": "ml_catalog",
                    "schema": "features",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "ml_feature_eng",
                },
            ],
            "blast_radius_score": 8,
            "blast_radius_note": (
                "Feeds 3 gold reporting tables (dashboards + executive reports) "
                "and the ML feature store. Failure here stalls BI and model retraining."
            ),
        },
        "prod_catalog.gold.kpi_daily": {
            "table_name": "prod_catalog.gold.kpi_daily",
            "upstreams": [
                {
                    "table_name": "prod_catalog.silver.events",
                    "catalog": "prod_catalog",
                    "schema": "silver",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "aggregate_gold",
                },
                {
                    "table_name": "prod_catalog.silver.user_profiles",
                    "catalog": "prod_catalog",
                    "schema": "silver",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "aggregate_gold",
                },
            ],
            "downstreams": [
                {
                    "table_name": "reporting.tableau.exec_dashboard",
                    "catalog": "reporting",
                    "schema": "tableau",
                    "table_type": "EXTERNAL",
                    "relationship": "READ_BY",
                    "job_name": None,
                },
                {
                    "table_name": "reporting.superset.ops_dashboard",
                    "catalog": "reporting",
                    "schema": "superset",
                    "table_type": "EXTERNAL",
                    "relationship": "READ_BY",
                    "job_name": None,
                },
            ],
            "blast_radius_score": 6,
            "blast_radius_note": (
                "Consumed by executive Tableau dashboard and ops Superset dashboard. "
                "Stale data here is visible to leadership within the hour."
            ),
        },
        "prod_catalog.silver.user_profiles": {
            "table_name": "prod_catalog.silver.user_profiles",
            "upstreams": [
                {
                    "table_name": "prod_catalog.bronze.raw_clickstream",
                    "catalog": "prod_catalog",
                    "schema": "bronze",
                    "table_type": "EXTERNAL",
                    "relationship": "DERIVED_FROM",
                    "job_name": "transform_silver",
                }
            ],
            "downstreams": [
                {
                    "table_name": "prod_catalog.gold.kpi_daily",
                    "catalog": "prod_catalog",
                    "schema": "gold",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "aggregate_gold",
                },
                {
                    "table_name": "ml_catalog.features.user_event_features",
                    "catalog": "ml_catalog",
                    "schema": "features",
                    "table_type": "MANAGED",
                    "relationship": "DERIVED_FROM",
                    "job_name": "ml_feature_eng",
                },
            ],
            "blast_radius_score": 5,
            "blast_radius_note": (
                "Joins into gold KPI table and ML feature store. "
                "Schema drift propagates to model training pipelines."
            ),
        },
    }

    result = lineage_map.get(table_name)
    if result is None:
        return {
            "table_name": table_name,
            "upstreams": [],
            "downstreams": [],
            "blast_radius_score": 0,
            "blast_radius_note": "Table not found in lineage registry.",
        }
    return result
