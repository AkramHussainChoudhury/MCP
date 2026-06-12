"""
Tests for mock_data.get_dlt_health()

What we verify:
  - Response shape including nested events list
  - Filtering by pipeline_id
  - Event severity levels are valid
  - Backlog metrics are non-negative
  - The stalled pipeline has a non-zero backlog
"""

import pytest
from mock_data import get_dlt_health
from tests.conftest import KNOWN_PIPELINE_IDS, UNKNOWN_ID

VALID_LEVELS   = {"INFO", "WARN", "ERROR"}
VALID_STATES   = {"RUNNING", "IDLE", "FAILED", "STOPPED", "DELETING"}
VALID_UPDATE_STATES = {"COMPLETED", "FAILED", "RUNNING", "WAITING_FOR_RESOURCES", "CANCELED"}


# ── Shape tests ──────────────────────────────────────────────────────────────

def test_returns_dict_with_pipelines_key():
    result = get_dlt_health()
    assert isinstance(result, dict)
    assert "pipelines" in result

def test_each_pipeline_has_required_fields():
    required = {"pipeline_id", "name", "state", "latest_update", "metrics", "events"}
    for pipeline in get_dlt_health()["pipelines"]:
        missing = required - pipeline.keys()
        assert not missing, f"Pipeline {pipeline.get('name')} missing: {missing}"

def test_latest_update_has_required_fields():
    required = {"update_id", "state", "full_refresh"}
    for pipeline in get_dlt_health()["pipelines"]:
        missing = required - pipeline["latest_update"].keys()
        assert not missing, (
            f"Pipeline {pipeline['name']} latest_update missing: {missing}"
        )

def test_each_event_has_required_fields():
    required = {"id", "timestamp", "level", "message"}
    for pipeline in get_dlt_health()["pipelines"]:
        for event in pipeline["events"]:
            missing = required - event.keys()
            assert not missing, (
                f"Event in pipeline {pipeline['name']} missing: {missing}"
            )


# ── Filter tests ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,pipeline_id", KNOWN_PIPELINE_IDS.items())
def test_filter_by_pipeline_id(name, pipeline_id):
    result = get_dlt_health(pipeline_id=pipeline_id)
    assert len(result["pipelines"]) == 1
    assert result["pipelines"][0]["pipeline_id"] == pipeline_id

def test_no_filter_returns_all_pipelines():
    result = get_dlt_health()
    assert len(result["pipelines"]) == len(KNOWN_PIPELINE_IDS)

def test_unknown_pipeline_id_returns_empty():
    result = get_dlt_health(pipeline_id=UNKNOWN_ID)
    assert result["pipelines"] == []


# ── Invariant tests ──────────────────────────────────────────────────────────

def test_event_levels_are_valid():
    for pipeline in get_dlt_health()["pipelines"]:
        for event in pipeline["events"]:
            assert event["level"] in VALID_LEVELS, (
                f"Unknown event level '{event['level']}' in pipeline {pipeline['name']}"
            )

def test_pipeline_states_are_valid():
    for pipeline in get_dlt_health()["pipelines"]:
        assert pipeline["state"] in VALID_STATES, (
            f"Unknown pipeline state '{pipeline['state']}'"
        )

def test_update_states_are_valid():
    for pipeline in get_dlt_health()["pipelines"]:
        state = pipeline["latest_update"]["state"]
        assert state in VALID_UPDATE_STATES, (
            f"Unknown update state '{state}' in pipeline {pipeline['name']}"
        )

def test_backlog_metrics_are_non_negative():
    for pipeline in get_dlt_health()["pipelines"]:
        m = pipeline["metrics"]
        assert m["backlog_bytes"] >= 0
        assert m["backlog_files"] >= 0
        assert m["num_queued_updates"] >= 0

def test_stalled_pipeline_has_nonzero_backlog():
    """The mock incident: bronze_to_silver is stalled with a 4 GB backlog."""
    pipeline_id = KNOWN_PIPELINE_IDS["bronze_to_silver"]
    result = get_dlt_health(pipeline_id=pipeline_id)
    pipeline = result["pipelines"][0]
    assert pipeline["metrics"]["backlog_bytes"] > 0, (
        "bronze_to_silver should have a non-zero backlog in mock data"
    )
