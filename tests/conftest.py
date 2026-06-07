from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest

from etl.config import Settings
from etl.snowflake_loader import SnowflakeLoader
from etl.storage import LocalFSBackend


@pytest.fixture
def tmp_lake(tmp_path):
    return LocalFSBackend(root_dir=str(tmp_path / "lake"))


@pytest.fixture
def frozen_settings(tmp_path):
    return Settings(
        storage_backend="local",
        local_lake_root=str(tmp_path / "lake"),
        dry_run=True,
        chunk_size=5,
    )


@pytest.fixture
def mock_snowflake_loader():
    return MagicMock(spec=SnowflakeLoader)


@pytest.fixture
def sample_users_df():
    return pd.DataFrame(
        {
            "user_id": [1, 2, 3, 3, 5],  # 3 is a duplicate
            "user_name": ["Alice", "Bob", "Carol", "Carol", None],  # null name
            "email": ["a@x.com", "b@x.com", "invalid_email", "c@x.com", "e@x.com"],
            "signup_date": ["2024-01-01", "2024-02-01", "invalid_date", "2024-02-01", "2024-03-01"],
        }
    )


@pytest.fixture
def sample_products_df():
    return pd.DataFrame(
        {
            "product_id": [10, 20, 30, 30, 50],  # 30 is a duplicate
            "product_name": ["Widget", "Gadget", "Gizmo", "Gizmo", "Doohickey"],
            "category": ["Electronics", "Home", "Books", "Books", "Sports"],
            "price": [9.99, None, 19.99, 19.99, 49.99],  # null price
        }
    )


@pytest.fixture
def sample_weblogs_df():
    return pd.DataFrame(
        {
            "log_id": [1, 2, 3, 4, 5, 1],  # 1 repeats (cross/intra-chunk dup)
            "user_id": [1, 2, None, 999, 5, 1],  # null + orphan (999)
            "product_id": [10, 20, 30, 40, 999, 10],  # orphan (999, 40 not in products either)
            "session_id": ["sess_1", "sess_1", None, "sess_2", "sess_2", "sess_1"],  # null session
            "action": ["view", "purchase", "view", "bogus_action", "add_to_cart", "view"],
            "timestamp": [
                "2024-06-01T10:00:00",
                "2024-06-01T10:05:00",
                "2024-06-01T10:10:00",
                "invalid_timestamp",
                "2024-06-01T10:15:00",
                "2024-06-01T10:00:00",
            ],
        }
    )
