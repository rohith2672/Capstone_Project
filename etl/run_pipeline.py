"""CLI entry point: builds the storage backend and Snowflake loader from Settings
(via the factories in storage.py / snowflake_loader.py — the only place credentials
are read and live clients constructed) and runs WebLogProcessor end to end.

Usage:
    python -m etl.run_pipeline                  # uses .env / environment, real backends
    python -m etl.run_pipeline --dry-run         # forces NullSnowflakeLoader (logs SQL, no connection)
    python -m etl.run_pipeline --storage local    # forces LocalFSBackend regardless of STORAGE_BACKEND
    python -m etl.run_pipeline --chunk-size 5000  # overrides CHUNK_SIZE
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import replace

from etl.config import load_settings
from etl.logging_setup import configure_logging, get_logger
from etl.processor import WebLogProcessor
from etl.snowflake_loader import get_snowflake_loader
from etl.storage import get_storage_backend


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the E-Commerce Web Log ETL pipeline (Bronze -> Silver -> Gold).")
    parser.add_argument("--dry-run", action="store_true", help="Force NullSnowflakeLoader — log SQL instead of executing it.")
    parser.add_argument("--storage", choices=["local", "s3"], default=None, help="Override STORAGE_BACKEND for this run.")
    parser.add_argument("--chunk-size", type=int, default=None, help="Override CHUNK_SIZE (rows per weblogs chunk).")
    parser.add_argument("--env-file", default=None, help="Path to a .env file to load (defaults to python-dotenv's discovery).")
    return parser.parse_args(argv)


def build_processor(args: argparse.Namespace) -> WebLogProcessor:
    settings = load_settings(args.env_file, dry_run=args.dry_run or None)
    if args.storage is not None:
        settings = replace(settings, storage_backend=args.storage)
    if args.chunk_size is not None:
        settings = replace(settings, chunk_size=args.chunk_size)

    storage = get_storage_backend(settings)
    snowflake_loader = get_snowflake_loader(settings, force_null=args.dry_run)

    return WebLogProcessor(
        weblog_file=settings.weblog_file,
        users_file=settings.users_file,
        products_file=settings.products_file,
        storage=storage,
        snowflake_loader=snowflake_loader,
        settings=settings,
    )


def main(argv=None) -> int:
    configure_logging()
    logger = get_logger(__name__)
    args = parse_args(argv)

    processor = build_processor(args)
    logger.info(
        "pipeline.start",
        extra={
            "etl_run_id": processor.etl_run_id,
            "storage_backend": processor.settings.storage_backend,
            "dry_run": processor.settings.dry_run,
            "chunk_size": processor.settings.chunk_size,
        },
    )

    try:
        metrics = processor.run()
    except Exception:
        logger.exception("pipeline.failed", extra={"etl_run_id": processor.etl_run_id})
        return 1

    logger.info(
        "pipeline.finished",
        extra={
            "etl_run_id": processor.etl_run_id,
            "status": metrics.status,
            "rows_extracted": metrics.total_rows_extracted(),
            "rows_quarantined": metrics.total_rows_quarantined(),
            "rows_loaded": metrics.total_rows_loaded(),
        },
    )
    return 0 if metrics.status == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
