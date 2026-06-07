from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from etl import helpers


# ---------------------------------------------------------------------------
# validate_schema / find_duplicates / dedupe_keep_first
# ---------------------------------------------------------------------------
def test_validate_schema_flags_nulls_in_required_columns():
    df = pd.DataFrame({"a": [1, None, 3], "b": [1, 2, None]})
    mask = helpers.validate_schema(df, ["a", "b"])
    assert mask.tolist() == [True, False, False]


def test_validate_schema_raises_on_missing_column():
    df = pd.DataFrame({"a": [1, 2]})
    with pytest.raises(ValueError, match="Missing required columns"):
        helpers.validate_schema(df, ["a", "b"])


def test_find_duplicates_intra_series():
    series = pd.Series([1, 2, 1, 3, 2, None])
    mask = helpers.find_duplicates(series)
    assert mask.tolist() == [False, False, True, False, True, False]


def test_find_duplicates_cross_chunk_via_seen_set():
    series = pd.Series([10, 11, 12])
    mask = helpers.find_duplicates(series, seen={11, 99})
    assert mask.tolist() == [False, True, False]


def test_dedupe_keep_first():
    df = pd.DataFrame({"id": [1, 1, 2, 3, 3], "val": ["a", "b", "c", "d", "e"]})
    deduped = helpers.dedupe_keep_first(df, subset=["id"])
    assert deduped["val"].tolist() == ["a", "c", "d"]


# ---------------------------------------------------------------------------
# parse_timestamp / categorize_action / sort_by_timestamp_per_session
# ---------------------------------------------------------------------------
def test_parse_timestamp_coerces_invalid_to_nat():
    series = pd.Series(["2024-06-01T10:00:00", "invalid_timestamp", None])
    parsed = helpers.parse_timestamp(series)
    assert parsed.notna().tolist() == [True, False, False]
    assert parsed.iloc[0] == pd.Timestamp("2024-06-01T10:00:00")


def test_is_valid_email_flags_malformed_and_missing():
    series = pd.Series(["a@example.com", "invalid_email", None, "no-at-sign.com", "b@x.co"])
    mask = helpers.is_valid_email(series)
    assert mask.tolist() == [True, False, False, False, True]


def test_categorize_action_normalizes_and_flags_unknown():
    series = pd.Series([" View ", "PURCHASE", "add_to_cart", "bogus"])
    categorized = helpers.categorize_action(series)
    assert categorized.tolist()[:3] == ["view", "purchase", "add_to_cart"]
    assert pd.isna(categorized.iloc[3])


def test_sort_by_timestamp_per_session_handles_out_of_order():
    df = pd.DataFrame(
        {
            "session_id": ["s1", "s1", "s2", "s1"],
            "action_ts": pd.to_datetime(
                ["2024-01-01T10:02", "2024-01-01T10:00", "2024-01-01T09:00", "2024-01-01T10:01"]
            ),
            "marker": ["c", "a", "x", "b"],
        }
    )
    sorted_df = helpers.sort_by_timestamp_per_session(df)
    assert sorted_df.loc[sorted_df["session_id"] == "s1", "marker"].tolist() == ["a", "b", "c"]


# ---------------------------------------------------------------------------
# Session metric helpers
# ---------------------------------------------------------------------------
def _session_frame():
    return pd.DataFrame(
        {
            "session_id": ["s1", "s1", "s1", "s2", "s2", "s3"],
            "action_ts": pd.to_datetime(
                [
                    "2024-01-01T10:00:00",
                    "2024-01-01T10:05:00",
                    "2024-01-01T10:10:00",
                    "2024-01-01T11:00:00",
                    "2024-01-01T11:01:00",
                    "2024-01-01T12:00:00",
                ]
            ),
            "action": ["view", "add_to_cart", "purchase", "view", "view", "view"],
        }
    )


def test_compute_session_durations_vectorized():
    durations = helpers.compute_session_durations(_session_frame())
    row = durations.set_index("session_id").loc["s1"]
    assert np.isclose(row["session_duration_s"], 600.0)  # 10 minutes
    row2 = durations.set_index("session_id").loc["s2"]
    assert np.isclose(row2["session_duration_s"], 60.0)
    row3 = durations.set_index("session_id").loc["s3"]
    assert np.isclose(row3["session_duration_s"], 0.0)


def test_compute_session_metrics_aggregates_correctly():
    metrics = helpers.compute_session_metrics(_session_frame()).set_index("session_id")

    assert metrics.loc["s1", "total_actions"] == 3
    assert metrics.loc["s1", "total_views"] == 1
    assert metrics.loc["s1", "total_cart_adds"] == 1
    assert metrics.loc["s1", "total_purchases"] == 1
    assert np.isclose(metrics.loc["s1", "conversion_rate"], 1.0)
    assert metrics.loc["s1", "is_abandoned_cart"] == False  # purchased

    assert metrics.loc["s2", "total_purchases"] == 0
    assert metrics.loc["s2", "is_abandoned_cart"] == False  # no cart adds either
    assert pd.isna(metrics.loc["s2", "conversion_rate"]) == False
    assert metrics.loc["s2", "conversion_rate"] == 0.0


def test_compute_conversion_rate_handles_div_by_zero():
    rate = helpers.compute_conversion_rate(pd.Series([2, 0, 5]), pd.Series([4, 0, 0]))
    assert rate.iloc[0] == 0.5
    assert pd.isna(rate.iloc[1])
    assert pd.isna(rate.iloc[2])


def test_flag_abandoned_carts():
    flags = helpers.flag_abandoned_carts(pd.Series([3, 0, 2]), pd.Series([0, 0, 1]))
    assert flags.tolist() == [True, False, False]


def test_flag_high_activity_threshold():
    flags = helpers.flag_high_activity(pd.Series([10, 51, 50]), threshold=50)
    assert flags.tolist() == [False, True, False]


def test_flag_long_sessions_uses_std_dev_threshold():
    # A single huge outlier among several "normal" values: with enough normal samples,
    # the outlier still clears mean + 2*std even though it also inflates both.
    durations = pd.Series([100.0, 105.0, 95.0, 102.0, 98.0, 101.0, 99.0, 1000.0])
    flags = helpers.flag_long_sessions(durations, std_devs=2.0)
    assert flags.tolist() == [False, False, False, False, False, False, False, True]


# ---------------------------------------------------------------------------
# Quarantine helpers
# ---------------------------------------------------------------------------
def test_quarantine_path_format():
    path = helpers.quarantine_path("weblogs", "2024-06-07", "abc-123")
    assert path == "quarantine/source=weblogs/etl_run_date=2024-06-07/etl_run_id=abc-123/anomalies.parquet"


def test_build_quarantine_frame_has_expected_columns_and_json_raw_row():
    rejected = pd.DataFrame({"log_id": [1, 2], "user_id": [None, 5]}, index=[10, 11])
    reasons = pd.Series(["null user_id", "duplicate log_id"], index=[10, 11])

    frame = helpers.build_quarantine_frame(
        rejected,
        reasons,
        source_file="weblogs.csv",
        etl_run_id="run-1",
        etl_run_date="2024-06-07",
        etl_run_timestamp="2024-06-07T10:00:00",
    )

    assert list(frame.columns) == list(helpers.QUARANTINE_COLUMNS)
    assert frame["row_index"].tolist() == [10, 11]
    assert frame["rejection_reason"].tolist() == ["null user_id", "duplicate log_id"]
    # raw_row must be valid JSON that round-trips the original row
    decoded = json.loads(frame["raw_row"].iloc[0])
    assert decoded["log_id"] == 1


def test_build_quarantine_frame_empty_input_returns_empty_with_columns():
    frame = helpers.build_quarantine_frame(
        pd.DataFrame(columns=["log_id"]),
        pd.Series([], dtype="object"),
        source_file="weblogs.csv",
        etl_run_id="run-1",
        etl_run_date="2024-06-07",
        etl_run_timestamp="2024-06-07T10:00:00",
    )
    assert frame.empty
    assert list(frame.columns) == list(helpers.QUARANTINE_COLUMNS)
