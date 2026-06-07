"""Tests for WebLogProcessor's Silver-phase methods: transform/enrich/write_silver.

Bronze is seeded directly onto the storage backend (mirroring what extract() would
have produced) so these tests exercise transform()'s "re-read Bronze via storage"
design in isolation from Bronze-phase validation.
"""
from __future__ import annotations

import pandas as pd
import pytest

from etl.processor import WebLogProcessor
from etl.storage import make_key


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


@pytest.fixture
def seeded_processor(processor, tmp_lake):
    """Seed Bronze layer directly — two chunks of weblogs (out-of-order within a
    session, to exercise sort_by_timestamp_per_session) plus users/products snapshots.
    """
    run_date = processor.etl_run_date

    weblogs_chunk1 = pd.DataFrame(
        {
            "log_id": [1, 2, 3],
            "user_id": [1, 1, 2],
            "product_id": [10, 20, 10],
            "session_id": ["s1", "s1", "s2"],
            "action": ["purchase", "view", "view"],  # s1 out of order: purchase logged before view
            "timestamp": [
                "2024-06-01T10:10:00",  # s1 action 2 chronologically (purchase, later)
                "2024-06-01T10:00:00",  # s1 action 1 chronologically (view, earlier)
                "2024-06-01T11:00:00",  # s2 action 1
            ],
        }
    )
    weblogs_chunk2 = pd.DataFrame(
        {
            "log_id": [4, 5],
            "user_id": [2, 1],
            "product_id": [20, 10],
            "session_id": ["s2", "s1"],
            "action": [" ADD_TO_CART ", "purchase"],  # exercises categorize_action normalization
            "timestamp": [
                "2024-06-01T11:05:00",  # s2 action 2
                "2024-06-01T10:20:00",  # s1 action 3 (another purchase — distinct from log_id 1)
            ],
        }
    )
    tmp_lake.write_parquet(
        weblogs_chunk1, make_key("bronze", "weblogs", {"ingest_date": run_date}, "weblogs_chunk_001.parquet")
    )
    tmp_lake.write_parquet(
        weblogs_chunk2, make_key("bronze", "weblogs", {"ingest_date": run_date}, "weblogs_chunk_002.parquet")
    )

    users = pd.DataFrame(
        {
            "user_id": [1, 2],
            "user_name": ["Alice", "Bob"],
            "email": ["alice@x.com", "bob@x.com"],
            "signup_date": ["2024-01-01", "2024-02-01"],
        }
    )
    products = pd.DataFrame(
        {
            "product_id": [10, 20],
            "product_name": ["Widget", "Gadget"],
            "category": ["Electronics", "Home"],
            "price": [9.99, 19.99],
        }
    )
    tmp_lake.write_parquet(users, make_key("bronze", "users", {"ingest_date": run_date}, "users.parquet"))
    tmp_lake.write_parquet(products, make_key("bronze", "products", {"ingest_date": run_date}, "products.parquet"))

    return processor


# ---------------------------------------------------------------------------
# transform()
# ---------------------------------------------------------------------------
def test_transform_reads_bronze_parses_categorizes_and_sorts(seeded_processor):
    seeded_processor.transform()
    weblogs = seeded_processor._silver_weblogs

    assert len(weblogs) == 5
    assert pd.api.types.is_datetime64_any_dtype(weblogs["action_ts"])

    # categorize_action normalizes whitespace/case
    assert weblogs.loc[weblogs["log_id"] == 4, "action"].iloc[0] == "add_to_cart"

    # out-of-order logs sorted chronologically per session: s1 should read view, purchase, purchase
    s1 = weblogs.loc[weblogs["session_id"] == "s1"]
    assert s1["action"].tolist() == ["view", "purchase", "purchase"]
    assert s1["action_ts"].is_monotonic_increasing


def test_transform_computes_hand_verified_session_metrics(seeded_processor):
    seeded_processor.transform()
    metrics = seeded_processor._session_metrics_df.set_index("session_id")

    # s1: view@10:00, purchase@10:10, purchase@10:20 -> duration 1200s, 1 view, 2 purchases
    assert metrics.loc["s1", "session_duration_s"] == pytest.approx(1200.0)
    assert metrics.loc["s1", "total_actions"] == 3
    assert metrics.loc["s1", "total_views"] == 1
    assert metrics.loc["s1", "total_cart_adds"] == 0
    assert metrics.loc["s1", "total_purchases"] == 2
    assert metrics.loc["s1", "conversion_rate"] == pytest.approx(2.0)
    assert metrics.loc["s1", "is_abandoned_cart"] == False

    # s2: view@11:00, add_to_cart@11:05 -> duration 300s, abandoned cart (cart add, no purchase)
    assert metrics.loc["s2", "session_duration_s"] == pytest.approx(300.0)
    assert metrics.loc["s2", "total_actions"] == 2
    assert metrics.loc["s2", "total_cart_adds"] == 1
    assert metrics.loc["s2", "total_purchases"] == 0
    assert metrics.loc["s2", "is_abandoned_cart"] == True


def test_transform_records_session_anomaly_counts(seeded_processor):
    seeded_processor.transform()
    counts = seeded_processor.metrics.session_anomaly_counts

    assert counts["abandoned_cart"] == 1   # s2
    assert counts["high_activity"] == 0    # neither session exceeds 50 actions
    assert "long_session" in counts


# ---------------------------------------------------------------------------
# enrich()
# ---------------------------------------------------------------------------
def test_enrich_joins_user_and_product_descriptive_fields(seeded_processor):
    seeded_processor.transform()
    seeded_processor.enrich()
    enriched = seeded_processor._silver_weblogs

    for col in ("user_name", "email", "product_name", "category", "price"):
        assert col in enriched.columns

    row = enriched.loc[enriched["log_id"] == 1].iloc[0]
    assert row["user_name"] == "Alice"
    assert row["product_name"] == "Widget"
    assert row["category"] == "Electronics"
    assert row["price"] == pytest.approx(9.99)


# ---------------------------------------------------------------------------
# write_silver()
# ---------------------------------------------------------------------------
def test_write_silver_persists_each_dataset_to_its_partitioned_layout(seeded_processor, tmp_lake):
    seeded_processor.transform()
    seeded_processor.enrich()

    seeded_processor.write_silver(seeded_processor._silver_weblogs, dataset="weblogs_clean")
    seeded_processor.write_silver(seeded_processor._users_clean, dataset="users_clean")
    seeded_processor.write_silver(seeded_processor._products_clean, dataset="products_clean")

    run_date = seeded_processor.etl_run_date
    run_id = seeded_processor.etl_run_id

    weblogs_key = f"silver/weblogs_clean/etl_run_date={run_date}/etl_run_id={run_id}/weblogs_silver.parquet"
    users_key = f"silver/users_clean/etl_run_date={run_date}/users_silver.parquet"
    products_key = f"silver/products_clean/etl_run_date={run_date}/products_silver.parquet"

    assert tmp_lake.exists(weblogs_key)
    assert tmp_lake.exists(users_key)
    assert tmp_lake.exists(products_key)

    roundtrip = tmp_lake.read_parquet(weblogs_key)
    assert len(roundtrip) == len(seeded_processor._silver_weblogs)
    assert seeded_processor.metrics.loaded["silver"]["weblogs_clean"] == len(roundtrip)
    assert seeded_processor.metrics.loaded["silver"]["users_clean"] == 2
    assert seeded_processor.metrics.loaded["silver"]["products_clean"] == 2


def test_write_silver_skips_empty_frame(seeded_processor):
    seeded_processor.write_silver(pd.DataFrame(columns=["session_id"]), dataset="weblogs_clean")
    assert seeded_processor.metrics.loaded["silver"].get("weblogs_clean", 0) == 0


def test_write_silver_unknown_dataset_raises(seeded_processor):
    with pytest.raises(ValueError, match="Unknown silver dataset"):
        seeded_processor.write_silver(pd.DataFrame({"a": [1]}), dataset="bogus_dataset")
