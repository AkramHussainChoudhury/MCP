"""
Tests for mock_data.get_cluster_health()

What we verify:
  - Response shape and required metric fields
  - Filtering by cluster_id
  - Metric values are within physically plausible ranges
  - The unhealthy cluster (etl_main) is correctly represented
"""

import pytest
from mock_data import get_cluster_health
from tests.conftest import KNOWN_CLUSTER_IDS, UNKNOWN_ID

METRIC_FIELDS = {
    "cpu_percent",
    "memory_used_mb",
    "memory_total_mb",
    "memory_percent",
    "active_tasks",
    "failed_tasks_last_hour",
    "dbu_consumed_last_hour",
    "gc_time_percent",
}


# ── Shape tests ──────────────────────────────────────────────────────────────

def test_returns_dict_with_clusters_key():
    result = get_cluster_health()
    assert isinstance(result, dict)
    assert "clusters" in result

def test_each_cluster_has_required_fields():
    required = {"cluster_id", "cluster_name", "state", "metrics"}
    for cluster in get_cluster_health()["clusters"]:
        missing = required - cluster.keys()
        assert not missing, f"Cluster {cluster.get('cluster_id')} missing: {missing}"

def test_each_cluster_has_all_metric_fields():
    for cluster in get_cluster_health()["clusters"]:
        missing = METRIC_FIELDS - cluster["metrics"].keys()
        assert not missing, (
            f"Cluster {cluster['cluster_id']} metrics missing: {missing}"
        )


# ── Filter tests ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,cluster_id", KNOWN_CLUSTER_IDS.items())
def test_filter_by_cluster_id(name, cluster_id):
    result = get_cluster_health(cluster_id=cluster_id)
    assert len(result["clusters"]) == 1
    assert result["clusters"][0]["cluster_id"] == cluster_id

def test_no_filter_returns_all_clusters():
    result = get_cluster_health()
    assert len(result["clusters"]) == len(KNOWN_CLUSTER_IDS)

def test_unknown_cluster_id_returns_empty():
    result = get_cluster_health(cluster_id=UNKNOWN_ID)
    assert result["clusters"] == []


# ── Invariant tests ──────────────────────────────────────────────────────────

def test_cpu_percent_in_valid_range():
    for cluster in get_cluster_health()["clusters"]:
        cpu = cluster["metrics"]["cpu_percent"]
        assert 0.0 <= cpu <= 100.0, f"cpu_percent {cpu} out of range for {cluster['cluster_id']}"

def test_memory_used_never_exceeds_total():
    for cluster in get_cluster_health()["clusters"]:
        m = cluster["metrics"]
        assert m["memory_used_mb"] <= m["memory_total_mb"], (
            f"memory_used_mb > memory_total_mb for cluster {cluster['cluster_id']}"
        )

def test_memory_percent_consistent_with_used_and_total():
    for cluster in get_cluster_health()["clusters"]:
        m = cluster["metrics"]
        if m["memory_total_mb"] > 0:
            expected = round(m["memory_used_mb"] / m["memory_total_mb"] * 100, 1)
            assert abs(m["memory_percent"] - expected) < 1.0, (
                f"memory_percent {m['memory_percent']} inconsistent with used/total "
                f"for cluster {cluster['cluster_id']}"
            )

def test_etl_main_cluster_is_under_high_memory_pressure():
    """The mock incident: etl_main is the unhealthy cluster driving OOM failures."""
    result = get_cluster_health(cluster_id=KNOWN_CLUSTER_IDS["etl_main"])
    cluster = result["clusters"][0]
    assert cluster["metrics"]["memory_percent"] > 80, (
        "etl_main should have high memory pressure in mock data"
    )

def test_terminated_cluster_has_zero_active_tasks():
    for cluster in get_cluster_health()["clusters"]:
        if cluster["state"] == "TERMINATED":
            assert cluster["metrics"]["active_tasks"] == 0
