"""Storage backend abstraction.

WebLogProcessor never constructs boto3 clients itself — it talks only to a
StorageBackend. This is what lets the entire pipeline run and be tested locally:
LocalFSBackend is a faithful stand-in that mirrors the EXACT same S3 key layout the
spec defines (e.g. "bronze/weblogs/ingest_date=2024-06-07/weblogs_chunk_001.parquet")
on local disk, so swapping to S3Backend later changes nothing about the pipeline logic
— only which backend the factory returns.
"""
from __future__ import annotations

import io
import os
from typing import Protocol, runtime_checkable

import pandas as pd

from etl.config import Settings


def make_key(layer: str, dataset: str, partitions: dict[str, str], filename: str) -> str:
    """Build a partitioned object key, e.g.
    make_key("bronze", "weblogs", {"ingest_date": "2024-06-07"}, "weblogs_chunk_001.parquet")
      -> "bronze/weblogs/ingest_date=2024-06-07/weblogs_chunk_001.parquet"
    """
    partition_segments = "/".join(f"{k}={v}" for k, v in partitions.items())
    parts = [layer, dataset]
    if partition_segments:
        parts.append(partition_segments)
    parts.append(filename)
    return "/".join(parts)


@runtime_checkable
class StorageBackend(Protocol):
    """Minimal interface WebLogProcessor needs from a Bronze/Silver/Quarantine store."""

    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        ...

    def read_parquet(self, key: str) -> pd.DataFrame:
        ...

    def exists(self, key: str) -> bool:
        ...

    def list(self, prefix: str) -> list[str]:
        ...

    def read_parquet_prefix(self, prefix: str) -> pd.DataFrame:
        ...


class LocalFSBackend:
    """Writes/reads Parquet under <root_dir>/<key>, using the key string verbatim as
    a relative path. Default backend — used by --dry-run, the sample run, and tests.
    """

    def __init__(self, root_dir: str = "data/lake"):
        self.root_dir = root_dir

    def _path(self, key: str) -> str:
        return os.path.join(self.root_dir, *key.split("/"))

    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False)
        return path

    def read_parquet(self, key: str) -> pd.DataFrame:
        return pd.read_parquet(self._path(key))

    def exists(self, key: str) -> bool:
        return os.path.exists(self._path(key))

    def list(self, prefix: str) -> list[str]:
        prefix_path = self._path(prefix)
        if not os.path.isdir(prefix_path):
            return []
        keys = []
        for dirpath, _dirnames, filenames in os.walk(prefix_path):
            for name in filenames:
                full = os.path.join(dirpath, name)
                rel = os.path.relpath(full, self.root_dir).replace(os.sep, "/")
                keys.append(rel)
        return sorted(keys)

    def read_parquet_prefix(self, prefix: str) -> pd.DataFrame:
        keys = [k for k in self.list(prefix) if k.endswith(".parquet")]
        if not keys:
            return pd.DataFrame()
        return pd.concat([self.read_parquet(k) for k in keys], ignore_index=True)


class S3Backend:
    """Wraps boto3 upload_fileobj/download_fileobj with TransferConfig for
    multi-threaded uploads (Phase 7 performance requirement). Uses the SAME key
    strings as S3 object keys — no path translation needed vs. LocalFSBackend.
    """

    def __init__(self, bucket: str, client=None, transfer_config=None):
        if client is None:
            import boto3
            client = boto3.client("s3")
        if transfer_config is None:
            from boto3.s3.transfer import TransferConfig
            transfer_config = TransferConfig(
                multipart_threshold=8 * 1024 * 1024,
                max_concurrency=10,
                multipart_chunksize=8 * 1024 * 1024,
                use_threads=True,
            )
        self.bucket = bucket
        self.client = client
        self.transfer_config = transfer_config

    def write_parquet(self, df: pd.DataFrame, key: str) -> str:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        self.client.upload_fileobj(buffer, self.bucket, key, Config=self.transfer_config)
        return f"s3://{self.bucket}/{key}"

    def read_parquet(self, key: str) -> pd.DataFrame:
        buffer = io.BytesIO()
        self.client.download_fileobj(self.bucket, key, buffer, Config=self.transfer_config)
        buffer.seek(0)
        return pd.read_parquet(buffer)

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False

    def list(self, prefix: str) -> list[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return sorted(keys)

    def read_parquet_prefix(self, prefix: str) -> pd.DataFrame:
        keys = [k for k in self.list(prefix) if k.endswith(".parquet")]
        if not keys:
            return pd.DataFrame()
        return pd.concat([self.read_parquet(k) for k in keys], ignore_index=True)


def get_storage_backend(settings: Settings) -> StorageBackend:
    """Factory: STORAGE_BACKEND=local|s3 picks the implementation; everything else
    about WebLogProcessor stays identical regardless of which one is selected.
    """
    if settings.storage_backend == "s3":
        settings.require_s3()
        import boto3
        client = boto3.client(
            "s3",
            region_name=settings.aws_region,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )
        return S3Backend(bucket=settings.s3_bucket, client=client)
    return LocalFSBackend(root_dir=settings.local_lake_root)
