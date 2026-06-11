"""Tests for WebLogProcessor's Bronze-phase validation, including the two
correctness traps: cross-chunk duplicate log_id detection and orphan-ID detection
against pre-built reference sets.
"""
from __future__ import annotations

import json

import pandas as pd
import pytest

from etl import helpers
from etl.processor import WebLogProcessor


@pytest.fixture
def processor(tmp_lake, mock_snowflake_loader, frozen_settings):
    return WebLogProcessor(
        weblog_file="data/raw/weblogs.csv",
        users_file="data/raw/users.csv",
        products_file="data/raw/products.csv",
        storage=tmp_lake,
        snowflake_loader=mock_snowflake_loader,
        settings=frozen_settings,
    )


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------
def test_validate_users_classifies_rows_correctly(processor, sample_users_df):
    clean, quarantine = processor.validate(sample_users_df, source="users")

    # Duplicate user_id=3 is kept (latest occurrence), not rejected.
    assert clean["user_id"].tolist() == [1, 2, 3, 5]
    assert quarantine["rejection_reason"].tolist() == []
    assert list(quarantine.columns) == list(helpers.QUARANTINE_COLUMNS)

    # Invalid/duplicate emails are flagged on clean rows, not rejected.
    assert clean["is_invalid_email"].tolist() == [False, False, False, False]
    assert clean["is_duplicate_email"].tolist() == [False, False, False, False]

    # Missing user_name is filled with "unknown", not left null.
    assert clean.loc[clean["user_id"] == 5, "user_name"].iloc[0] == "unknown"


def test_validate_users_fills_missing_name_with_unknown(processor):
    df = pd.DataFrame(
        {
            "user_id": [1, 2],
            "user_name": [None, "Bob"],
            "email": ["a@x.com", "b@x.com"],
            "signup_date": ["2024-01-01", "2024-02-01"],
        }
    )
    clean, _ = processor.validate(df, source="users")

    assert clean.loc[clean["user_id"] == 1, "user_name"].iloc[0] == "unknown"
    assert clean.loc[clean["user_id"] == 2, "user_name"].iloc[0] == "Bob"


def test_validate_users_flags_invalid_email_not_rejects(processor):
    df = pd.DataFrame(
        {
            "user_id": [1, 2],
            "user_name": ["Alice", "Bob"],
            "email": ["invalid_email", "b@x.com"],
            "signup_date": ["2024-01-01", "2024-02-01"],
        }
    )
    clean, quarantine = processor.validate(df, source="users")

    assert clean["user_id"].tolist() == [1, 2]
    assert quarantine.empty
    assert clean.loc[clean["user_id"] == 1, "is_invalid_email"].iloc[0] == True
    assert clean.loc[clean["user_id"] == 2, "is_invalid_email"].iloc[0] == False


# ---------------------------------------------------------------------------
# Products
# ---------------------------------------------------------------------------
def test_validate_products_classifies_rows_correctly(processor, sample_products_df):
    clean, quarantine = processor.validate(sample_products_df, source="products")

    # Duplicate product_id=30 is kept (latest occurrence), not rejected.
    assert clean["product_id"].tolist() == [10, 20, 30, 50]
    assert quarantine["rejection_reason"].tolist() == []

    # Missing price is filled with 0, not left null.
    assert clean.loc[clean["product_id"] == 20, "price"].iloc[0] == 0.0


def test_validate_products_fills_missing_price_with_zero(processor):
    df = pd.DataFrame(
        {
            "product_id": [1, 2],
            "product_name": ["Widget", "Gadget"],
            "category": ["Electronics", "Home"],
            "price": [None, 9.99],
        }
    )
    clean, _ = processor.validate(df, source="products")

    assert clean.loc[clean["product_id"] == 1, "price"].iloc[0] == 0.0
    assert clean.loc[clean["product_id"] == 2, "price"].iloc[0] == 9.99


# ---------------------------------------------------------------------------
# Weblogs (requires reference sets — mirrors what extract() builds before streaming)
# ---------------------------------------------------------------------------
def test_validate_weblogs_classifies_rows_correctly(processor, sample_weblogs_df):
    processor.valid_user_ids = frozenset({1, 2, 3, 5})
    processor.valid_product_ids = frozenset({10, 20, 30, 50})

    clean, quarantine = processor.validate(sample_weblogs_df, source="weblogs")

    assert clean["log_id"].tolist() == [1, 2]
    assert quarantine["rejection_reason"].tolist() == [
        "null user_id",
        "orphan user_id",
        "orphan product_id",
        "duplicate log_id",
    ]


def test_validate_weblogs_cross_chunk_duplicate_log_id_detected(processor, sample_weblogs_df):
    """Trap (b): a log_id that was kept in an earlier chunk must be flagged as a
    duplicate when it reappears in a later chunk — per-chunk .duplicated() alone
    would miss this."""
    processor.valid_user_ids = frozenset({1, 2, 3, 5})
    processor.valid_product_ids = frozenset({10, 20, 30, 50})

    # First chunk seeds self._seen_log_ids with {1, 2, 3, 4, 5} (all non-duplicate ids)
    processor.validate(sample_weblogs_df, source="weblogs")
    assert processor._seen_log_ids == {1, 2, 3, 4, 5}

    second_chunk = pd.DataFrame(
        {
            "log_id": [2, 100],  # 2 repeats from the first chunk; 100 is new
            "user_id": [1, 2],
            "product_id": [10, 20],
            "session_id": ["sess_1", "sess_3"],
            "action": ["view", "view"],
            "timestamp": ["2024-06-01T11:00:00", "2024-06-01T11:05:00"],
        }
    )
    clean, quarantine = processor.validate(second_chunk, source="weblogs")

    assert clean["log_id"].tolist() == [100]
    assert quarantine["rejection_reason"].tolist() == ["duplicate log_id"]
    assert json.loads(quarantine["raw_row"].iloc[0])["log_id"] == 2
    assert 100 in processor._seen_log_ids


def test_validate_unknown_source_raises(processor):
    with pytest.raises(ValueError, match="Unknown validation source"):
        processor.validate(pd.DataFrame(), source="bogus")


# ---------------------------------------------------------------------------
# write_bronze / write_quarantine
# ---------------------------------------------------------------------------
def test_write_bronze_persists_clean_rows_and_records_metrics(processor, sample_users_df, tmp_lake):
    clean, _ = processor.validate(sample_users_df, source="users")
    processor.write_bronze(clean, source="users")

    key = f"bronze/users/ingest_date={processor.etl_run_date}/users.parquet"
    assert tmp_lake.exists(key)
    roundtrip = tmp_lake.read_parquet(key)
    assert roundtrip["user_id"].tolist() == clean["user_id"].tolist()
    assert processor.metrics.loaded["bronze"]["users"] == len(clean)


def test_write_bronze_chunked_weblogs_uses_indexed_filename(processor, sample_weblogs_df, tmp_lake):
    processor.valid_user_ids = frozenset({1, 2, 3, 5})
    processor.valid_product_ids = frozenset({10, 20, 30, 50})
    clean, _ = processor.validate(sample_weblogs_df, source="weblogs")
    processor.write_bronze(clean, source="weblogs", chunk_index=1)

    key = f"bronze/weblogs/ingest_date={processor.etl_run_date}/weblogs_chunk_001.parquet"
    assert tmp_lake.exists(key)


def test_write_quarantine_persists_rejected_rows_and_records_metrics(processor, tmp_lake):
    df = pd.DataFrame(
        {
            "product_id": [10, None],
            "product_name": ["Widget", "Gizmo"],
            "category": ["Electronics", "Books"],
            "price": [9.99, 19.99],
        }
    )
    _, quarantine = processor.validate(df, source="products")
    processor.write_quarantine(quarantine, source="products")

    key = f"quarantine/source=products/etl_run_date={processor.etl_run_date}/etl_run_id={processor.etl_run_id}/anomalies.parquet"
    assert tmp_lake.exists(key)
    assert processor.metrics.quarantined["products"] == len(quarantine)
    assert processor.metrics.rejection_reason_counts["products"]["null product_id"] == 1
    assert processor.metrics.quarantine_paths["products"]


def test_write_bronze_skips_empty_frame(processor, tmp_lake):
    processor.write_bronze(pd.DataFrame(columns=["user_id"]), source="users")
    assert processor.metrics.loaded["bronze"].get("users", 0) == 0


def test_write_quarantine_skips_empty_frame(processor, tmp_lake):
    processor.write_quarantine(pd.DataFrame(columns=["rejection_reason"]), source="users")
    assert processor.metrics.quarantined.get("users", 0) == 0
