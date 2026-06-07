"""End-to-end pipeline integration test: WebLogProcessor.run() against the REAL
generated CSVs in data/raw/, using LocalFSBackend + NullSnowflakeLoader (the
--dry-run combination). This is the strongest local proof that Bronze -> Silver ->
Gold orchestration, quarantine, metrics, and report generation all cohere —
the one thing it can't prove is live S3/Snowflake connectivity (see README).
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from etl import helpers
from etl.config import Settings
from etl.processor import WebLogProcessor
from etl.snowflake_loader import NullSnowflakeLoader
from etl.storage import LocalFSBackend, make_key

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SQL_DIR = PROJECT_ROOT / "sql"


@pytest.fixture
def real_csvs_available():
    paths = [RAW_DIR / "weblogs.csv", RAW_DIR / "users.csv", RAW_DIR / "products.csv"]
    if not all(p.is_file() for p in paths):
        pytest.skip("Sample data not generated — run etl/generate_sample_data.py first")
    return paths


@pytest.fixture
def dry_run_processor(tmp_path, monkeypatch, real_csvs_available):
    """A WebLogProcessor wired exactly like `run_pipeline.py --dry-run` would build
    it, but pointed at an isolated tmp_path lake/report dir so the test never
    touches the real project's data/lake, run_reports/, or data_quality_report.md."""
    monkeypatch.chdir(tmp_path)

    weblog_file, users_file, products_file = real_csvs_available
    settings = Settings(
        storage_backend="local",
        local_lake_root=str(tmp_path / "lake"),
        sql_dir=str(SQL_DIR),
        report_dir=str(tmp_path / "run_reports"),
        chunk_size=5000,
        dry_run=True,
    )
    storage = LocalFSBackend(root_dir=settings.local_lake_root)
    loader = NullSnowflakeLoader()

    return WebLogProcessor(
        weblog_file=str(weblog_file),
        users_file=str(users_file),
        products_file=str(products_file),
        storage=storage,
        snowflake_loader=loader,
        settings=settings,
    )


def test_run_end_to_end_against_real_sample_data(dry_run_processor, tmp_path):
    proc = dry_run_processor
    metrics = proc.run()

    # ---- overall outcome -------------------------------------------------
    assert metrics.status == "SUCCESS"
    assert metrics.error_message is None

    # ---- conservation: every extracted row is either kept or quarantined -
    for source in ("users", "products", "weblogs"):
        extracted = metrics.extracted[source]
        kept = metrics.loaded["bronze"].get(source, 0)
        quarantined = metrics.quarantined.get(source, 0)
        assert extracted > 0
        assert kept + quarantined == extracted, source

    # The sample generator deliberately seeds defects (e.g. "invalid_timestamp"
    # literal strings visible in the raw CSV) — the run must catch and quarantine some.
    assert metrics.total_rows_quarantined() > 0
    assert "invalid timestamp" in metrics.rejection_reason_counts["weblogs"]

    # ---- Bronze: clean rows actually persisted to the lake ---------------
    ingest_prefix = make_key("bronze", "weblogs", {"ingest_date": proc.etl_run_date}, "").rstrip("/")
    bronze_weblogs = proc.storage.read_parquet_prefix(ingest_prefix)
    assert len(bronze_weblogs) == metrics.loaded["bronze"]["weblogs"]
    assert len(bronze_weblogs) == len(set(bronze_weblogs["log_id"]))  # no dup log_ids survived

    bronze_users = proc.storage.read_parquet(
        make_key("bronze", "users", {"ingest_date": proc.etl_run_date}, "users.parquet")
    )
    bronze_products = proc.storage.read_parquet(
        make_key("bronze", "products", {"ingest_date": proc.etl_run_date}, "products.parquet")
    )
    assert set(bronze_weblogs["user_id"].astype(int)) <= set(bronze_users["user_id"].astype(int))
    assert set(bronze_weblogs["product_id"].astype(int)) <= set(bronze_products["product_id"].astype(int))

    # ---- Quarantine: rejects persisted with the 7-column spec schema -----
    quarantine_weblogs = proc.storage.read_parquet_prefix(f"quarantine/source=weblogs/etl_run_date={proc.etl_run_date}/etl_run_id={proc.etl_run_id}")
    assert len(quarantine_weblogs) == metrics.quarantined["weblogs"]
    assert set(quarantine_weblogs.columns) == set(helpers.QUARANTINE_COLUMNS)

    # ---- Silver: transformed/enriched output persisted -------------------
    silver_weblogs = proc.storage.read_parquet(
        make_key(
            "silver", "weblogs_clean",
            {"etl_run_date": proc.etl_run_date, "etl_run_id": proc.etl_run_id},
            "weblogs_silver.parquet",
        )
    )
    assert len(silver_weblogs) == metrics.loaded["silver"]["weblogs_clean"]
    assert len(silver_weblogs) == len(bronze_weblogs)  # Silver re-reads ALL of Bronze's clean rows
    # transformations applied
    assert pd.api.types.is_datetime64_any_dtype(silver_weblogs["action_ts"])
    assert set(silver_weblogs["action"].dropna().unique()) <= set(helpers.VALID_ACTIONS)
    # run metadata embedded directly in the rows (so Gold SQL needs no path-parsing)
    assert (silver_weblogs["etl_run_id"] == proc.etl_run_id).all()
    assert (silver_weblogs["etl_run_date"] == proc.etl_run_date).all()
    # enrichment joins brought in reference columns
    for col in ("user_name", "email", "product_name", "category", "price"):
        assert col in silver_weblogs.columns

    silver_users = proc.storage.read_parquet(
        make_key("silver", "users_clean", {"etl_run_date": proc.etl_run_date}, "users_silver.parquet")
    )
    silver_products = proc.storage.read_parquet(
        make_key("silver", "products_clean", {"etl_run_date": proc.etl_run_date}, "products_silver.parquet")
    )
    assert len(silver_users) == metrics.loaded["silver"]["users_clean"]
    assert len(silver_products) == metrics.loaded["silver"]["products_clean"]

    # ---- Gold: dry-run still exercises build_gold (against NullSnowflakeLoader) --
    for table in ("DIM_USER", "DIM_PRODUCT", "FACT_USER_ACTIVITY", "AGG_SESSION_METRICS"):
        assert table in metrics.loaded["gold"]

    # ---- Validation: skipped (no live Snowflake connection) --------------
    assert len(metrics.validation_results) == 3
    assert all(r["status"] == "SKIPPED" for r in metrics.validation_results)

    # ---- session-level anomalies recorded (sanity, not exact values) -----
    for kind in ("abandoned_cart", "high_activity", "long_session"):
        assert kind in metrics.session_anomaly_counts
        assert metrics.session_anomaly_counts[kind] >= 0

    # ---- the data_quality_report.md deliverable was written --------------
    root_report = tmp_path / "data_quality_report.md"
    assert root_report.is_file()
    text = root_report.read_text(encoding="utf-8")
    assert f"Run `{proc.etl_run_id}`" in text
    assert "**Status:** SUCCESS" in text
    assert "orphan user_id" in text or "duplicate log_id" in text  # some rejection reason surfaced

    history_dir = tmp_path / "run_reports"
    history_files = list(history_dir.glob(f"data_quality_report_*_{proc.etl_run_id}.md"))
    assert len(history_files) == 1
    assert history_files[0].read_text(encoding="utf-8") == text


def test_run_records_audit_rows_via_null_loader(dry_run_processor):
    """_write_audit_log reads the real insert_audit_log.sql and calls executemany —
    NullSnowflakeLoader just logs + echoes len(rows), proving the call succeeds
    with the real SQL file and the real RunMetrics-shaped rows (no live warehouse)."""
    proc = dry_run_processor
    metrics = proc.run()

    # to_audit_rows is the exact payload _write_audit_log sent to executemany
    rows = metrics.to_audit_rows(source_file=proc.weblog_file)
    assert {r["layer"] for r in rows} == {"bronze", "silver", "gold"}
    assert all(r["status"] == "SUCCESS" for r in rows)
    assert all(r["etl_run_id"] == proc.etl_run_id for r in rows)
