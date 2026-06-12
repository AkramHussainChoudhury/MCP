"""
Tests for mock_data.get_lineage()

What we verify:
  - Response shape for known and unknown tables
  - blast_radius_score is within the documented 1–10 range
  - Upstream and downstream entries have required fields
  - Unknown table returns a safe empty response, not an error
  - The high-blast-radius tables have appropriate scores
"""

import pytest
from mock_data import get_lineage
from tests.conftest import KNOWN_TABLES, UNKNOWN_ID

RELATIONSHIP_TYPES = {"DERIVED_FROM", "READ_BY", "WRITTEN_BY"}


# ── Shape tests ──────────────────────────────────────────────────────────────

def test_returns_dict_with_required_keys():
    required = {"table_name", "upstreams", "downstreams", "blast_radius_score", "blast_radius_note"}
    result = get_lineage(KNOWN_TABLES[0])
    missing = required - result.keys()
    assert not missing, f"Lineage response missing keys: {missing}"

def test_upstream_entries_have_required_fields():
    required = {"table_name", "catalog", "schema", "table_type", "relationship"}
    for table in KNOWN_TABLES:
        for upstream in get_lineage(table)["upstreams"]:
            missing = required - upstream.keys()
            assert not missing, (
                f"Upstream entry for {table} missing: {missing}"
            )

def test_downstream_entries_have_required_fields():
    required = {"table_name", "catalog", "schema", "table_type", "relationship"}
    for table in KNOWN_TABLES:
        for downstream in get_lineage(table)["downstreams"]:
            missing = required - downstream.keys()
            assert not missing, (
                f"Downstream entry for {table} missing: {missing}"
            )


# ── Unknown table ────────────────────────────────────────────────────────────

def test_unknown_table_returns_empty_lists():
    result = get_lineage(UNKNOWN_ID)
    assert result["upstreams"] == []
    assert result["downstreams"] == []

def test_unknown_table_returns_zero_blast_radius():
    result = get_lineage(UNKNOWN_ID)
    assert result["blast_radius_score"] == 0

def test_unknown_table_does_not_raise():
    # Should return a safe default, never raise KeyError or similar
    try:
        get_lineage(UNKNOWN_ID)
    except Exception as exc:
        pytest.fail(f"get_lineage raised an unexpected exception: {exc}")


# ── Invariant tests ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("table_name", KNOWN_TABLES)
def test_blast_radius_score_in_valid_range(table_name):
    result = get_lineage(table_name)
    score = result["blast_radius_score"]
    assert 0 <= score <= 10, (
        f"blast_radius_score {score} out of range for {table_name}"
    )

@pytest.mark.parametrize("table_name", KNOWN_TABLES)
def test_blast_radius_note_is_non_empty_string(table_name):
    result = get_lineage(table_name)
    assert isinstance(result["blast_radius_note"], str)
    assert len(result["blast_radius_note"]) > 0

def test_relationship_types_are_valid():
    for table in KNOWN_TABLES:
        result = get_lineage(table)
        for entry in result["upstreams"] + result["downstreams"]:
            assert entry["relationship"] in RELATIONSHIP_TYPES, (
                f"Unknown relationship type '{entry['relationship']}' in {table}"
            )

def test_raw_clickstream_has_highest_blast_radius():
    """bronze.raw_clickstream is the root source — it should have the highest blast radius."""
    scores = {table: get_lineage(table)["blast_radius_score"] for table in KNOWN_TABLES}
    raw = scores["prod_catalog.bronze.raw_clickstream"]
    others = [s for t, s in scores.items() if t != "prod_catalog.bronze.raw_clickstream"]
    assert all(raw >= s for s in others), (
        f"raw_clickstream score {raw} should be >= all other scores: {scores}"
    )

def test_gold_table_has_no_managed_downstreams():
    """kpi_daily feeds dashboards (EXTERNAL), not other managed Delta tables."""
    result = get_lineage("prod_catalog.gold.kpi_daily")
    managed_downstreams = [
        d for d in result["downstreams"] if d["table_type"] == "MANAGED"
    ]
    assert managed_downstreams == [], (
        "gold.kpi_daily should not feed other MANAGED tables — it is a terminal table"
    )
