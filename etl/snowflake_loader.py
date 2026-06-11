"""Snowflake I/O isolated behind a small, mockable interface.

WebLogProcessor only ever calls methods on a `loader: SnowflakeLoader`-shaped object —
it never imports snowflake.connector itself. That is what lets the full pipeline run
and be asserted against in tests via unittest.mock.MagicMock(spec=SnowflakeLoader),
and lets --dry-run swap in NullSnowflakeLoader with zero branching in the processor.

All SQL TEXT lives in sql/*.sql files (loaded via run_sql_file), not inlined in
Python — keeps the SQL deliverables independently reviewable and avoids drift between
"the SQL we submit" and "the SQL we actually run".
"""
from __future__ import annotations

import logging
import re
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_STATEMENT_SPLIT_RE = re.compile(r";\s*(?:\n|$)")


def _split_statements(sql_text: str) -> list[str]:
    """Split a .sql file's contents into individual statements on statement-terminating
    semicolons (end of line), stripping blank/comment-only fragments."""
    statements = []
    for raw in _STATEMENT_SPLIT_RE.split(sql_text):
        stmt = raw.strip()
        if not stmt:
            continue
        # drop pure-comment fragments (e.g. a trailing "-- note" after the last statement)
        non_comment_lines = [
            line for line in stmt.splitlines() if line.strip() and not line.strip().startswith("--")
        ]
        if non_comment_lines:
            statements.append(stmt)
    return statements


class SnowflakeLoader:
    """Thin wrapper around snowflake.connector. `connection_factory` is injected so
    tests can patch it without touching the real connector.
    """

    def __init__(self, conn_params: dict, connection_factory=None):
        self.conn_params = conn_params
        self._connection_factory = connection_factory or self._default_connection_factory
        self._conn = None

    @staticmethod
    def _default_connection_factory(conn_params: dict):
        import snowflake.connector
        return snowflake.connector.connect(**conn_params)

    # ------------------------------------------------------------------
    def connect(self):
        if self._conn is None:
            self._conn = self._connection_factory(self.conn_params)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    # ------------------------------------------------------------------
    def execute(self, sql: str, params: dict | tuple | None = None):
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            try:
                return cursor.fetchall()
            except Exception:
                return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, sql: str, rows: list[dict] | list[tuple]):
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, rows)
            return cursor.rowcount
        finally:
            cursor.close()

    def run_sql_file(self, path: str | Path, params: dict | tuple | None = None) -> list:
        """Load a .sql file from disk, split it into statements, run each in order,
        and return the results of the LAST statement (the typical "report the row
        count / SELECT result of the final statement" pattern for migration-style files).
        """
        sql_text = Path(path).read_text(encoding="utf-8")
        results = []
        for statement in _split_statements(sql_text):
            results.append(self.execute(statement, params))
        return results[-1] if results else []

    # ------------------------------------------------------------------
    def merge_dim_user(self, sql_path: str | Path) -> int:
        return self._affected_rows(self.run_sql_file(sql_path))

    def merge_dim_product(self, sql_path: str | Path) -> int:
        return self._affected_rows(self.run_sql_file(sql_path))

    @staticmethod
    def _affected_rows(result) -> int:
        if isinstance(result, int):
            return result
        try:
            return len(result)
        except TypeError:
            return 0


class NullSnowflakeLoader:
    """Drop-in replacement used in --dry-run mode or when Snowflake credentials are
    absent. Logs what it WOULD do and returns deterministic synthetic counts so
    run() can be exercised end-to-end with no live connection at all.
    """

    def __init__(self, conn_params: dict | None = None):
        self.conn_params = conn_params or {}

    def connect(self):
        logger.info("[DRY RUN] would connect to Snowflake", extra={"conn_params_keys": list(self.conn_params)})
        return None

    def close(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass

    def execute(self, sql: str, params=None):
        logger.info("[DRY RUN] would execute SQL", extra={"sql_preview": sql[:200]})
        return []

    def executemany(self, sql: str, rows):
        logger.info("[DRY RUN] would executemany", extra={"sql_preview": sql[:200], "row_count": len(rows)})
        return len(rows)

    def run_sql_file(self, path, params=None):
        logger.info("[DRY RUN] would run SQL file", extra={"path": str(path)})
        return []

    def merge_dim_user(self, sql_path) -> int:
        logger.info("[DRY RUN] would MERGE INTO DIM_USER", extra={"sql_path": str(sql_path)})
        return 0

    def merge_dim_product(self, sql_path) -> int:
        logger.info("[DRY RUN] would MERGE INTO DIM_PRODUCT", extra={"sql_path": str(sql_path)})
        return 0


def get_snowflake_loader(settings, force_null: bool = False):
    """Factory: returns NullSnowflakeLoader when --dry-run is set or credentials are
    absent, else a real SnowflakeLoader. Mirrors get_storage_backend's role for storage.
    """
    if force_null or settings.dry_run or not settings.has_snowflake_credentials():
        return NullSnowflakeLoader(settings.snowflake_conn_params() if settings.has_snowflake_credentials() else {})
    return SnowflakeLoader(settings.snowflake_conn_params())
