"""Centralized settings loading. Credentials are read here ONLY — never elsewhere."""
from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv


class ConfigError(Exception):
    """Raised when required settings for the selected backend are missing."""


@dataclass(frozen=True)
class Settings:
    storage_backend: str = "local"
    local_lake_root: str = "data/lake"

    s3_bucket_name: str = "weatherdatastore78"
    s3_processed_prefix: str = "processed/weather"
    aws_region: str = "us-east-1"
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""

    snowflake_account: str = ""
    snowflake_user: str = ""
    snowflake_password: str = ""
    snowflake_role: str = ""
    snowflake_warehouse: str = "COMPUTE_WH"
    snowflake_database: str = "WEATHER_DB"
    snowflake_schema: str = "PUBLIC"
    snowflake_table: str = "weather_data"

    chunk_size: int = 10000
    weblog_file: str = "data/raw/weblogs.csv"
    users_file: str = "data/raw/users.csv"
    products_file: str = "data/raw/products.csv"
    sql_dir: str = "sql"
    report_dir: str = "run_reports"

    dry_run: bool = False

    def require_s3(self) -> None:
        missing = [
            name
            for name, value in (
                ("S3_BUCKET_NAME", self.s3_bucket_name),
                ("AWS_ACCESS_KEY_ID", self.aws_access_key_id),
                ("AWS_SECRET_ACCESS_KEY", self.aws_secret_access_key),
            )
            if not value
        ]
        if missing:
            raise ConfigError(
                f"STORAGE_BACKEND=s3 requires these environment variables: {', '.join(missing)}"
            )

    def has_snowflake_credentials(self) -> bool:
        return bool(
            self.snowflake_account and self.snowflake_user and self.snowflake_password
        )

    def snowflake_conn_params(self) -> dict:
        params = {
            "account": self.snowflake_account,
            "user": self.snowflake_user,
            "password": self.snowflake_password,
            "warehouse": self.snowflake_warehouse,
            "database": self.snowflake_database,
            "schema": self.snowflake_schema,
        }
        if self.snowflake_role:
            params["role"] = self.snowflake_role
        return params


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_settings(env_path: str | None = None, *, dry_run: bool | None = None) -> Settings:
    """Load settings from environment variables (and an optional .env file).

    STORAGE_BACKEND defaults to "local" so the pipeline runs out of the box with zero
    cloud credentials. Missing Snowflake/S3 credentials are only an error once the code
    that actually needs them is reached (see Settings.require_s3 / has_snowflake_credentials).
    """
    load_dotenv(env_path, override=False)

    settings = Settings(
        storage_backend=os.environ.get("STORAGE_BACKEND", "local").strip().lower(),
        local_lake_root=os.environ.get("LOCAL_LAKE_ROOT", "data/lake"),
        s3_bucket_name=os.environ.get("S3_BUCKET_NAME", "weatherdatastore78"),
        s3_processed_prefix=os.environ.get("S3_PROCESSED_PREFIX", "processed/weather"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", ""),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
        snowflake_account=os.environ.get("SNOWFLAKE_ACCOUNT", ""),
        snowflake_user=os.environ.get("SNOWFLAKE_USER", ""),
        snowflake_password=os.environ.get("SNOWFLAKE_PASSWORD", ""),
        snowflake_role=os.environ.get("SNOWFLAKE_ROLE", ""),
        snowflake_warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        snowflake_database=os.environ.get("SNOWFLAKE_DATABASE", "WEATHER_DB"),
        snowflake_schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        snowflake_table=os.environ.get("SNOWFLAKE_TABLE", "weather_data"),
        chunk_size=_env_int("CHUNK_SIZE", 10000),
        weblog_file=os.environ.get("WEBLOG_FILE", "data/raw/weblogs.csv"),
        users_file=os.environ.get("USERS_FILE", "data/raw/users.csv"),
        products_file=os.environ.get("PRODUCTS_FILE", "data/raw/products.csv"),
        sql_dir=os.environ.get("SQL_DIR", "sql"),
        report_dir=os.environ.get("REPORT_DIR", "run_reports"),
        dry_run=_env_bool("DRY_RUN", False) if dry_run is None else dry_run,
    )

    if settings.storage_backend not in {"local", "s3"}:
        raise ConfigError(
            f"STORAGE_BACKEND must be 'local' or 's3', got {settings.storage_backend!r}"
        )

    return settings
