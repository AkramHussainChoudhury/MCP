"""
Tests for mock_data.get_table_stats()

What we verify:
  - Response shape and required fields
  - Filtering by table_name
  - fragmentation_score is always between 0.0 and 1.0
  - Size and file count are positive
  - The fragmented table is correctly represented
"""

import pytest
from mock_data import get_table_stats
from tests.conftest import KNOWN_TABLES, UNKNOWN_ID


# ── Shape tests ──────────────────────────────────────────────────────────────

def test_returns_dict_with_tables_key():
    result = get_table_stats()
    assert isinstance(result, dict)
    assert "tables" in result

def test_each_table_has_required_fields():
    required = {
        "table_name", "data_source_format", "num_files", "size_bytes",
        "partitioning", "last_modified", "last_optimized", "last_vacuumed",
        "fragmentation_score",
    }
    for table in get_table_stats()["tables"]:
        missing = required - table.keys()
        assert not missing, f"Table {table.get('table_name')} missing: {missing}"


# ── Filter tests ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("table_name", KNOWN_TABLES)
def test_filter_by_table_name(table_name):
    result = get_table_stats(table_name=table_name)
    assert len(result["tables"]) == 1
    assert result["tables"][0]["table_name"] == table_name

def test_no_filter_returns_all_tables():
    result = get_table_stats()
    assert len(result["tables"]) == len(KNOWN_TABLES)

def test_unknown_table_returns_empty():
    result = get_table_stats(table_name=UNKNOWN_ID)
    assert result["tables"] == []


# ── Invariant tests ──────────────────────────────────────────────────────────

def test_fragmentation_score_between_0_and_1():
    for table in get_table_stats()["tables"]:
        score = table["fragmentation_score"]
        assert 0.0 <= score <= 1.0, (
            f"fragmentation_score {score} out of range for {table['table_name']}"
        )

def test_num_files_is_positive():
    for table in get_table_stats()["tables"]:
        assert table["num_files"] > 0, (
            f"num_files must be > 0 for {table['table_name']}"
        )

def test_size_bytes_is_positive():
    for table in get_table_stats()["tables"]:
        assert table["size_bytes"] > 0

def test_partitioning_is_a_list():
    for table in get_table_stats()["tables"]:
        assert isinstance(table["partitioning"], list)

def test_delta_format():
    for table in get_table_stats()["tables"]:
        assert table["data_source_format"] == "DELTA"

def test_silver_events_is_highly_fragmented():
    """The mock incident: silver.events has 28k files and hasn't been optimized in 30 days."""
    result = get_table_stats(table_name="prod_catalog.silver.events")
    table = result["tables"][0]
    assert table["fragmentation_score"] > 0.6, (
        "silver.events should be highly fragmented in mock data"
    )
    assert table["num_files"] > 10_000, (
        "silver.events should have a large number of small files"
    )
