"""Tests for RunMetrics (the audit/report accumulator) and etl.report's rendering
and persistence of data_quality_report.md.
"""
from __future__ import annotations

import os

import pandas as pd
import pytest

from etl import report
from etl.metrics import RunMetrics


@pytest.fixture
def metrics():
    return RunMetrics(etl_run_id="run-123", etl_run_date="2024-06-07", etl_run_timestamp="2024-06-07T10:00:00")


# ---------------------------------------------------------------------------
# RunMetrics
# ---------------------------------------------------------------------------
def test_record_extracted_and_quarantined_accumulate(metrics):
    metrics.record_extracted("weblogs", 100)
    metrics.record_extracted("weblogs", 50)
    metrics.record_quarantined("weblogs", 10, pd.Series(["null user_id"] * 6 + ["orphan user_id"] * 4))

    assert metrics.total_rows_extracted() == 150
    assert metrics.total_rows_quarantined() == 10
    assert metrics.rejection_reason_counts["weblogs"]["null user_id"] == 6
    assert metrics.rejection_reason_counts["weblogs"]["orphan user_id"] == 4


def test_record_loaded_is_keyed_by_layer_then_table(metrics):
    metrics.record_loaded("bronze", "weblogs", 80)
    metrics.record_loaded("bronze", "users", 20)
    metrics.record_loaded("silver", "weblogs_clean", 80)

    assert metrics.total_rows_loaded() == 180
    assert metrics.total_rows_loaded("bronze") == 100
    assert metrics.total_rows_loaded("silver") == 80
    assert metrics.total_rows_loaded("gold") == 0


def test_to_audit_rows_produces_one_row_per_medallion_layer(metrics):
    metrics.record_extracted("weblogs", 100)
    metrics.record_quarantined("weblogs", 10, pd.Series(["null user_id"] * 10))
    metrics.record_quarantine_path("weblogs", "quarantine/source=weblogs/.../anomalies.csv")
    metrics.record_loaded("bronze", "weblogs", 90)
    metrics.record_loaded("silver", "weblogs_clean", 90)
    metrics.record_loaded("gold", "FACT_USER_ACTIVITY", 90)
    metrics.finalize("SUCCESS")

    rows = metrics.to_audit_rows(source_file="data/raw/weblogs.csv")
    by_layer = {r["layer"]: r for r in rows}

    assert set(by_layer) == {"bronze", "silver", "gold"}

    # Extraction/quarantine totals attributed to bronze only (see to_audit_rows docstring)
    assert by_layer["bronze"]["rows_extracted"] == 100
    assert by_layer["bronze"]["rows_quarantined"] == 10
    assert by_layer["bronze"]["rows_loaded"] == 90
    assert "anomalies.csv" in by_layer["bronze"]["quarantine_s3_path"]

    assert by_layer["silver"]["rows_extracted"] == 0
    assert by_layer["silver"]["rows_quarantined"] == 0
    assert by_layer["silver"]["rows_loaded"] == 90
    assert by_layer["silver"]["quarantine_s3_path"] == ""

    assert by_layer["gold"]["rows_loaded"] == 90
    assert all(r["status"] == "SUCCESS" for r in rows)
    assert all(r["etl_run_id"] == "run-123" for r in rows)


def test_finalize_sets_status_and_error(metrics):
    metrics.finalize("FAILED", error="boom")
    assert metrics.status == "FAILED"
    assert metrics.error_message == "boom"


def test_as_dict_round_trips_nested_loaded_structure(metrics):
    metrics.record_loaded("bronze", "weblogs", 5)
    d = metrics.as_dict()
    assert d["loaded"] == {"bronze": {"weblogs": 5}}
    assert isinstance(d["loaded"]["bronze"], dict)  # not a defaultdict — JSON-serializable


# ---------------------------------------------------------------------------
# report.render_data_quality_report (pure — no I/O)
# ---------------------------------------------------------------------------
def _populated_metrics():
    m = RunMetrics(etl_run_id="run-abc", etl_run_date="2024-06-07", etl_run_timestamp="2024-06-07T10:00:00")
    m.record_extracted("weblogs", 1000)
    m.record_quarantined("weblogs", 150, pd.Series(["orphan user_id"] * 100 + ["invalid timestamp"] * 50))
    m.record_quarantine_path("weblogs", "quarantine/source=weblogs/etl_run_date=2024-06-07/etl_run_id=run-abc/anomalies.csv")
    m.record_loaded("bronze", "weblogs", 850)
    m.record_loaded("silver", "weblogs_clean", 850)
    m.record_loaded("gold", "FACT_USER_ACTIVITY", 850)
    m.record_session_anomaly("abandoned_cart", 42)
    m.record_session_anomaly("long_session", 7)
    m.set_quality_observations({"users.null_user_name_rate": 0.05})
    m.set_validation_results([{"check": "orphan_user_sk_check", "status": "SKIPPED", "result": None, "note": "dry-run"}])
    m.finalize("SUCCESS")
    return m


def test_render_data_quality_report_includes_all_required_sections():
    text = report.render_data_quality_report(_populated_metrics())

    for heading in (
        "# Data Quality Report",
        "## Row Counts",
        "## Quarantine Breakdown",
        "## Data Quality Observations",
        "## Session-Level Anomalies",
        "## Post-Load Validation Results",
        "## Recommendations",
    ):
        assert heading in text

    assert "run-abc" in text
    assert "orphan user_id" in text
    assert "150" in text  # quarantine count surfaces somewhere
    assert "abandoned_cart" in text
    assert "SKIPPED" in text


def test_render_data_quality_report_flags_high_quarantine_rate_in_recommendations():
    text = report.render_data_quality_report(_populated_metrics())
    # 150 / 1000 = 15% > 10% threshold
    assert "investigate upstream data quality" in text


def test_render_data_quality_report_handles_empty_run():
    m = RunMetrics(etl_run_id="run-empty", etl_run_date="2024-06-07", etl_run_timestamp="2024-06-07T10:00:00")
    m.finalize("SUCCESS")
    text = report.render_data_quality_report(m)

    assert "_No rows were quarantined this run._" in text
    assert "_None recorded._" in text
    assert "_Skipped — no Snowflake connection" in text


# ---------------------------------------------------------------------------
# report.write_report (I/O — persists to root + history dir)
# ---------------------------------------------------------------------------
def test_write_report_persists_to_root_and_history(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    class _Settings:
        report_dir = "run_reports"

    metrics = _populated_metrics()
    root_path, history_path = report.write_report(metrics, settings=_Settings())

    assert os.path.isfile(root_path)
    assert os.path.isfile(history_path)
    assert "run_reports" in history_path
    assert "run-abc" in history_path

    root_text = open(root_path, encoding="utf-8").read()
    history_text = open(history_path, encoding="utf-8").read()
    assert root_text == history_text
    assert "# Data Quality Report" in root_text
