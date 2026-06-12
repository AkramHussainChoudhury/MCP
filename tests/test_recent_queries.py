"""
Tests for mock_data.get_recent_queries()

What we verify:
  - Response shape and required fields
  - status_filter='FAILED' returns only failed queries
  - status_filter='SLOW'   returns only slow (> 30s) finished queries
  - limit parameter is respected
  - Failed queries always have an error_message
  - Successful queries never have an error_message
  - Duration and byte metrics are non-negative
"""

import pytest
from mock_data import get_recent_queries

SLOW_THRESHOLD_MS = 30_000


# ── Shape tests ──────────────────────────────────────────────────────────────

def test_returns_dict_with_queries_key():
    result = get_recent_queries()
    assert isinstance(result, dict)
    assert "queries" in result

def test_each_query_has_required_fields():
    required = {
        "query_id", "status", "query_text", "user_name",
        "duration_ms", "error_message", "metrics",
    }
    for query in get_recent_queries()["queries"]:
        missing = required - query.keys()
        assert not missing, f"Query {query.get('query_id')} missing: {missing}"

def test_metrics_has_required_subfields():
    required = {"read_bytes", "rows_read_count", "num_tasks"}
    for query in get_recent_queries()["queries"]:
        missing = required - query["metrics"].keys()
        assert not missing, (
            f"Query {query['query_id']} metrics missing: {missing}"
        )


# ── Filter tests ─────────────────────────────────────────────────────────────

def test_failed_filter_returns_only_failed_queries():
    result = get_recent_queries(status_filter="FAILED")
    for query in result["queries"]:
        assert query["status"] == "FAILED", (
            f"Query {query['query_id']} has status {query['status']}, expected FAILED"
        )

def test_failed_filter_returns_at_least_one_query():
    result = get_recent_queries(status_filter="FAILED")
    assert len(result["queries"]) > 0, "Expected at least one FAILED query in mock data"

def test_slow_filter_returns_only_slow_finished_queries():
    result = get_recent_queries(status_filter="SLOW")
    for query in result["queries"]:
        assert query["status"] == "FINISHED", (
            f"SLOW filter returned a non-FINISHED query: {query['query_id']}"
        )
        assert query["duration_ms"] > SLOW_THRESHOLD_MS, (
            f"Query {query['query_id']} duration {query['duration_ms']}ms "
            f"is not above slow threshold {SLOW_THRESHOLD_MS}ms"
        )

def test_no_filter_returns_mixed_statuses():
    result = get_recent_queries()
    statuses = {q["status"] for q in result["queries"]}
    assert len(statuses) > 1, "Expected both FAILED and FINISHED queries without filter"

def test_limit_is_respected():
    result = get_recent_queries(limit=2)
    assert len(result["queries"]) <= 2


# ── Invariant tests ──────────────────────────────────────────────────────────

def test_failed_queries_have_error_message():
    for query in get_recent_queries()["queries"]:
        if query["status"] == "FAILED":
            assert query["error_message"], (
                f"FAILED query {query['query_id']} has no error_message"
            )

def test_finished_queries_have_no_error_message():
    for query in get_recent_queries()["queries"]:
        if query["status"] == "FINISHED":
            assert not query["error_message"], (
                f"FINISHED query {query['query_id']} unexpectedly has an error_message"
            )

def test_duration_is_non_negative():
    for query in get_recent_queries()["queries"]:
        assert query["duration_ms"] >= 0

def test_read_bytes_non_negative():
    for query in get_recent_queries()["queries"]:
        assert query["metrics"]["read_bytes"] >= 0
