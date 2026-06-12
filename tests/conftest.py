"""
Shared constants used across test modules.
These mirror the IDs defined in mock_data.py — kept here so tests
don't import implementation details directly from the data layer.
"""

KNOWN_JOB_NAMES = ["ingest_raw", "transform_silver", "aggregate_gold", "ml_feature_eng"]

KNOWN_CLUSTER_IDS = {
    "etl_main":    "0601-084523-reef412",
    "ml_training": "0601-091200-teal889",
    "adhoc_sql":   "0601-072100-sand301",
}

KNOWN_PIPELINE_IDS = {
    "bronze_to_silver": "ple-8f3a2c11-4d90-47b1-a2e5-9b0f3c8d1e4f",
    "silver_to_gold":   "ple-2d7b9e44-1a23-4c56-8f01-3d2e7a9b5c0d",
}

KNOWN_TABLES = [
    "prod_catalog.silver.events",
    "prod_catalog.gold.kpi_daily",
    "prod_catalog.bronze.raw_clickstream",
    "prod_catalog.silver.user_profiles",
]

UNKNOWN_ID = "does-not-exist-xyz"
