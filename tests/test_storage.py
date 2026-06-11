from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pandas.testing as pdt

from etl.storage import LocalFSBackend, S3Backend, get_storage_backend, make_key
from etl.config import Settings


# ---------------------------------------------------------------------------
# make_key
# ---------------------------------------------------------------------------
def test_make_key_builds_partitioned_path_matching_spec_layout():
    key = make_key("bronze", "weblogs", {"ingest_date": "2024-06-07"}, "weblogs_chunk_001.csv")
    assert key == "bronze/weblogs/ingest_date=2024-06-07/weblogs_chunk_001.csv"


def test_make_key_supports_multiple_partitions():
    key = make_key(
        "quarantine",
        "source=weblogs",
        {"etl_run_date": "2024-06-07", "etl_run_id": "abc-123"},
        "anomalies.csv",
    )
    assert key == "quarantine/source=weblogs/etl_run_date=2024-06-07/etl_run_id=abc-123/anomalies.csv"


# ---------------------------------------------------------------------------
# LocalFSBackend round-trips
# ---------------------------------------------------------------------------
def test_local_backend_write_then_read_round_trips(tmp_lake):
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    key = "bronze/weblogs/ingest_date=2024-06-07/weblogs_chunk_001.csv"

    path = tmp_lake.write_csv(df, key)
    assert tmp_lake.exists(key)

    read_back = tmp_lake.read_csv(key)
    pdt.assert_frame_equal(read_back, df)


def test_local_backend_list_and_read_prefix_concatenates_chunks(tmp_lake):
    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [3, 4]})
    tmp_lake.write_csv(df1, "bronze/weblogs/ingest_date=2024-06-07/weblogs_chunk_001.csv")
    tmp_lake.write_csv(df2, "bronze/weblogs/ingest_date=2024-06-07/weblogs_chunk_002.csv")

    keys = tmp_lake.list("bronze/weblogs/ingest_date=2024-06-07")
    assert len(keys) == 2

    combined = tmp_lake.read_csv_prefix("bronze/weblogs/ingest_date=2024-06-07")
    assert combined["a"].tolist() == [1, 2, 3, 4]


def test_local_backend_exists_false_for_missing_key(tmp_lake):
    assert tmp_lake.exists("bronze/weblogs/ingest_date=2024-01-01/missing.csv") is False


def test_local_backend_read_csv_prefix_empty_returns_empty_frame(tmp_lake):
    result = tmp_lake.read_csv_prefix("silver/weblogs_clean/etl_run_date=2099-01-01")
    assert result.empty


# ---------------------------------------------------------------------------
# S3Backend (mocked boto3 — no network access)
# ---------------------------------------------------------------------------
def test_s3_backend_write_csv_uses_upload_fileobj_with_transfer_config():
    mock_client = MagicMock()
    backend = S3Backend(bucket="my-bucket", client=mock_client, transfer_config="CONFIG_SENTINEL")

    df = pd.DataFrame({"a": [1, 2]})
    result = backend.write_csv(df, "bronze/users/ingest_date=2024-06-07/users.csv")

    mock_client.upload_fileobj.assert_called_once()
    args, kwargs = mock_client.upload_fileobj.call_args
    assert args[1] == "my-bucket"
    assert args[2] == "bronze/users/ingest_date=2024-06-07/users.csv"
    assert kwargs["Config"] == "CONFIG_SENTINEL"
    assert result == "s3://my-bucket/bronze/users/ingest_date=2024-06-07/users.csv"


def test_s3_backend_list_paginates_objects():
    mock_client = MagicMock()
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "bronze/weblogs/a.csv"}, {"Key": "bronze/weblogs/b.csv"}]},
        {"Contents": [{"Key": "bronze/weblogs/c.csv"}]},
    ]
    mock_client.get_paginator.return_value = mock_paginator
    backend = S3Backend(bucket="my-bucket", client=mock_client, transfer_config="CONFIG_SENTINEL")

    keys = backend.list("bronze/weblogs")
    assert keys == ["bronze/weblogs/a.csv", "bronze/weblogs/b.csv", "bronze/weblogs/c.csv"]
    mock_client.get_paginator.assert_called_once_with("list_objects_v2")


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
def test_get_storage_backend_returns_local_by_default(tmp_path):
    settings = Settings(storage_backend="local", local_lake_root=str(tmp_path))
    backend = get_storage_backend(settings)
    assert isinstance(backend, LocalFSBackend)
    assert backend.root_dir == str(tmp_path)


def test_get_storage_backend_raises_clear_error_when_s3_creds_missing():
    settings = Settings(storage_backend="s3", s3_bucket_name="")
    try:
        get_storage_backend(settings)
        assert False, "expected ConfigError"
    except Exception as e:
        assert "S3_BUCKET_NAME" in str(e)


@patch("boto3.client")
def test_get_storage_backend_returns_s3_backend_when_configured(mock_boto_client):
    mock_boto_client.return_value = MagicMock()
    settings = Settings(
        storage_backend="s3",
        s3_bucket_name="my-bucket",
        aws_access_key_id="AKIA...",
        aws_secret_access_key="secret",
    )
    backend = get_storage_backend(settings)
    assert isinstance(backend, S3Backend)
    assert backend.bucket == "my-bucket"
