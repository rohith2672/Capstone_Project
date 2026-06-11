"""Pure functional helpers: validation, deduplication, parsing, metric calculation,
and quarantine row assembly. Every function here is a pure DataFrame/Series/scalar
transform with no I/O — this is what makes them trivially unit-testable in isolation
and is the codebase's "functional programming" surface.
"""
from __future__ import annotations

import json
import re

import numpy as np
import pandas as pd

VALID_ACTIONS = ("view", "add_to_cart", "purchase")

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

QUARANTINE_COLUMNS = (
    "etl_run_id",
    "etl_run_date",
    "etl_run_timestamp",
    "source_file",
    "row_index",
    "rejection_reason",
    "raw_row",
)


# ---------------------------------------------------------------------------
# Schema validation / missing values / deduplication
# ---------------------------------------------------------------------------
def validate_schema(df: pd.DataFrame, required_columns: list[str]) -> pd.Series:
    """Boolean mask: True where a row has no missing value in any required column.

    Column *presence* is a frame-level concern (raises, since a missing column means
    every row fails identically); per-row null checks are vectorized.
    """
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")
    return df[required_columns].notna().all(axis=1)


def find_duplicates(series: pd.Series, seen: set | None = None) -> pd.Series:
    """Boolean mask flagging duplicates both *within* this series and against a
    previously-seen set (e.g. log_ids from earlier chunks). Non-null values only —
    nulls are handled separately by validate_schema.
    """
    intra = series.duplicated(keep="first")
    if seen:
        cross = series.isin(seen)
        return (intra | cross) & series.notna()
    return intra & series.notna()


def dedupe_keep_first(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """Drop duplicate rows (keeping the first occurrence) on the given column subset."""
    return df.drop_duplicates(subset=subset, keep="first")


def dedupe_keep_latest(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """Drop duplicate rows on `subset`, silently keeping the LAST (highest-index)
    occurrence of each key — used where a repeated id is a dedup preference, not a
    data quality failure (e.g. duplicate user_id/product_id: keep the latest record).

    Rows with a null value in any `subset` column are left untouched (null-id rows
    are handled separately by the caller's reject-mask logic), since drop_duplicates
    would otherwise treat multiple NaNs as duplicates of one another.
    """
    null_mask = df[subset].isna().any(axis=1)
    non_null = df.loc[~null_mask]
    deduped = non_null.sort_index(ascending=False).drop_duplicates(subset=subset, keep="first")
    return pd.concat([deduped, df.loc[null_mask]]).sort_index()


# ---------------------------------------------------------------------------
# Parsing / categorization
# ---------------------------------------------------------------------------
def parse_timestamp(series: pd.Series) -> pd.Series:
    """Vectorized timestamp parsing — unparseable values become NaT (caller checks
    .isna() to route them to quarantine). No per-row try/except, no loops.
    """
    return pd.to_datetime(series, errors="coerce")


def is_valid_email(series: pd.Series) -> pd.Series:
    """Vectorized regex-based email format check -> boolean mask (NaN/None -> False)."""
    return series.astype("string").str.match(_EMAIL_RE, na=False)


def categorize_action(series: pd.Series) -> pd.Series:
    """Normalize action values; values outside VALID_ACTIONS become NaN so the
    caller can route them to quarantine via the same notna()-based validation path.
    """
    normalized = series.astype("string").str.strip().str.lower()
    return normalized.where(normalized.isin(VALID_ACTIONS))


def sort_by_timestamp_per_session(df: pd.DataFrame) -> pd.DataFrame:
    """Handle out-of-order logs: sort rows by timestamp within each session."""
    return df.sort_values(["session_id", "action_ts"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Session metrics (vectorized NumPy/Pandas — no explicit loops)
# ---------------------------------------------------------------------------
def compute_session_durations(df: pd.DataFrame) -> pd.DataFrame:
    """Per-session start/end/duration via a single vectorized groupby-agg.

    duration_s = (session_end - session_start) computed with NumPy datetime64 subtraction,
    converted to seconds via .dt.total_seconds() — fully vectorized, no Python loops.
    """
    bounds = df.groupby("session_id")["action_ts"].agg(session_start="min", session_end="max")
    bounds["session_duration_s"] = (
        bounds["session_end"] - bounds["session_start"]
    ).dt.total_seconds()
    return bounds.reset_index()


def compute_session_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Per-session aggregate: total actions + per-action-type counts, merged with durations."""
    counts = (
        df.groupby("session_id")["action"]
        .value_counts()
        .unstack(fill_value=0)
        .reindex(columns=list(VALID_ACTIONS), fill_value=0)
        .rename(columns={"view": "total_views", "add_to_cart": "total_cart_adds", "purchase": "total_purchases"})
    )
    counts["total_actions"] = counts[["total_views", "total_cart_adds", "total_purchases"]].sum(axis=1)
    counts = counts.reset_index()

    durations = compute_session_durations(df)
    metrics = durations.merge(counts, on="session_id", how="left")

    metrics["conversion_rate"] = compute_conversion_rate(metrics["total_purchases"], metrics["total_views"])
    metrics["is_abandoned_cart"] = flag_abandoned_carts(metrics["total_cart_adds"], metrics["total_purchases"])
    metrics["is_high_activity"] = flag_high_activity(metrics["total_actions"])
    return metrics


def compute_conversion_rate(total_purchases: pd.Series, total_views: pd.Series) -> pd.Series:
    """purchases / views, vectorized and division-by-zero safe (-> NaN when views == 0)."""
    return total_purchases / total_views.replace(0, np.nan)


def flag_abandoned_carts(total_cart_adds: pd.Series, total_purchases: pd.Series) -> pd.Series:
    """True where a session added to cart but never purchased."""
    return (total_cart_adds > 0) & (total_purchases == 0)


def flag_high_activity(total_actions: pd.Series, threshold: int = 50) -> pd.Series:
    """True where a session exceeds the high-activity action-count threshold."""
    return total_actions > threshold


def flag_long_sessions(durations_s: pd.Series, std_devs: float = 2.0) -> pd.Series:
    """True where a session duration exceeds `std_devs` standard deviations above the mean
    (mirrors the spec's 'unusually long sessions' SQL check, computed in pandas)."""
    threshold = durations_s.mean() + std_devs * durations_s.std()
    return durations_s > threshold


# ---------------------------------------------------------------------------
# Quarantine
# ---------------------------------------------------------------------------
def quarantine_path(source: str, etl_run_date: str, etl_run_id: str, chunk_index: int | None = None) -> str:
    """Build the quarantine object key:
    quarantine/source=<source>/etl_run_date=<date>/etl_run_id=<uuid>/anomalies.csv

    `chunk_index`, when given, produces `anomalies_chunk_NNN.csv` instead —
    weblogs streams in multiple chunks, each producing its own quarantine batch
    under the SAME etl_run_id partition; without a per-chunk filename, each
    write_quarantine() call would overwrite the previous chunk's anomalies at
    the same key (mirrors why write_bronze names files `<source>_chunk_NNN.csv`).
    """
    filename = f"anomalies_chunk_{chunk_index:03d}.csv" if chunk_index is not None else "anomalies.csv"
    return (
        f"quarantine/source={source}/"
        f"etl_run_date={etl_run_date}/"
        f"etl_run_id={etl_run_id}/"
        f"{filename}"
    )


def build_quarantine_frame(
    rejected: pd.DataFrame,
    reasons: pd.Series,
    *,
    source_file: str,
    etl_run_id: str,
    etl_run_date: str,
    etl_run_timestamp: str,
) -> pd.DataFrame:
    """Assemble the 7-column quarantine schema for a batch of rejected rows.

    raw_row is JSON-serialized per row via .apply — an honest, deliberate exception to
    "no loops": heterogeneous-row JSON serialization has no clean vectorized form.
    """
    if rejected.empty:
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)

    raw_rows = rejected.apply(lambda row: json.dumps(row.to_dict(), default=str), axis=1)

    return pd.DataFrame(
        {
            "etl_run_id": etl_run_id,
            "etl_run_date": etl_run_date,
            "etl_run_timestamp": etl_run_timestamp,
            "source_file": source_file,
            "row_index": rejected.index.to_numpy(),
            "rejection_reason": reasons.to_numpy(),
            "raw_row": raw_rows.to_numpy(),
        }
    ).reset_index(drop=True)
