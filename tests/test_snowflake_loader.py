from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from etl.snowflake_loader import (
    NullSnowflakeLoader,
    SnowflakeLoader,
    _split_statements,
    get_snowflake_loader,
)
from etl.config import Settings


def _loader_with_mock_connection():
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("ok",)]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    factory = MagicMock(return_value=mock_conn)
    loader = SnowflakeLoader(conn_params={"account": "x"}, connection_factory=factory)
    return loader, factory, mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# _split_statements
# ---------------------------------------------------------------------------
def test_split_statements_separates_on_terminating_semicolons():
    sql = "CREATE TABLE a (x INT);\n\nINSERT INTO a VALUES (1);\n-- trailing comment\n"
    statements = _split_statements(sql)
    assert len(statements) == 2
    assert statements[0].startswith("CREATE TABLE a")
    assert statements[1].startswith("INSERT INTO a")


# ---------------------------------------------------------------------------
# SnowflakeLoader — connection lifecycle, never touches the real connector
# ---------------------------------------------------------------------------
def test_connect_uses_injected_factory_and_caches_connection():
    loader, factory, mock_conn, _ = _loader_with_mock_connection()
    conn1 = loader.connect()
    conn2 = loader.connect()
    assert conn1 is conn2 is mock_conn
    factory.assert_called_once_with({"account": "x"})


def test_close_clears_cached_connection():
    loader, _, mock_conn, _ = _loader_with_mock_connection()
    loader.connect()
    loader.close()
    mock_conn.close.assert_called_once()
    assert loader._conn is None


def test_execute_runs_sql_and_returns_fetchall_results():
    loader, _, _, mock_cursor = _loader_with_mock_connection()
    result = loader.execute("SELECT 1")
    mock_cursor.execute.assert_called_once_with("SELECT 1", None)
    assert result == [("ok",)]
    mock_cursor.close.assert_called_once()


def test_executemany_invokes_cursor_executemany():
    loader, _, _, mock_cursor = _loader_with_mock_connection()
    mock_cursor.rowcount = 3
    rows = [{"a": 1}, {"a": 2}, {"a": 3}]
    result = loader.executemany("INSERT INTO t VALUES (%(a)s)", rows)
    mock_cursor.executemany.assert_called_once_with("INSERT INTO t VALUES (%(a)s)", rows)
    assert result == 3


# ---------------------------------------------------------------------------
# run_sql_file — splits and executes statements, returns last result
# ---------------------------------------------------------------------------
def test_run_sql_file_executes_each_statement_in_order(tmp_path):
    loader, _, _, mock_cursor = _loader_with_mock_connection()
    mock_cursor.fetchall.side_effect = [[("first",)], [("second",)]]

    sql_path = tmp_path / "script.sql"
    sql_path.write_text("SELECT 1;\nSELECT 2;\n", encoding="utf-8")

    result = loader.run_sql_file(sql_path)

    assert mock_cursor.execute.call_count == 2
    assert result == [("second",)]


# ---------------------------------------------------------------------------
# NullSnowflakeLoader — never imports/touches the real connector
# ---------------------------------------------------------------------------
def test_null_loader_returns_deterministic_synthetic_results():
    loader = NullSnowflakeLoader({"account": "x"})
    assert loader.execute("SELECT 1") == []
    assert loader.executemany("INSERT ...", [{"a": 1}, {"a": 2}]) == 2
    assert loader.merge_dim_user("sql/dml/merge_dim_user.sql") == 0
    # context manager protocol works without raising
    with loader as l:
        assert l is loader


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------
def test_get_snowflake_loader_returns_null_when_dry_run():
    settings = Settings(dry_run=True)
    assert isinstance(get_snowflake_loader(settings), NullSnowflakeLoader)


def test_get_snowflake_loader_returns_null_when_credentials_missing():
    settings = Settings(dry_run=False)
    assert isinstance(get_snowflake_loader(settings), NullSnowflakeLoader)


def test_get_snowflake_loader_returns_real_loader_when_credentials_present():
    settings = Settings(
        dry_run=False,
        snowflake_account="acct",
        snowflake_user="user",
        snowflake_password="pw",
        snowflake_warehouse="WH",
    )
    loader = get_snowflake_loader(settings)
    assert isinstance(loader, SnowflakeLoader)
    assert loader.conn_params["account"] == "acct"
