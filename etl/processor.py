"""WebLogProcessor — orchestrates the full Bronze -> Silver -> Gold pipeline.

Design notes (see README "Architecture Decisions" / "Assumptions" for full rationale):

- storage and snowflake_loader are INJECTED (never constructed here). This is what
  lets the whole pipeline run and be tested locally with LocalFSBackend / NullSnowflakeLoader,
  and swap to S3Backend / SnowflakeLoader via config alone.
- Bronze streams weblogs in chunks and writes clean rows immediately; Silver RE-READS
  Bronze's persisted output via storage (not in-memory chunk accumulation) — keeps each
  Medallion layer independently re-runnable.
- Two correctness traps are handled explicitly: (a) valid_user_ids/valid_product_ids
  reference sets are built from users/products BEFORE streaming weblogs, so orphan
  detection works; (b) self._seen_log_ids is threaded across validate() calls so
  duplicate log_ids that straddle chunk boundaries are still caught.
- validate() takes a `source` argument (a small, deliberate adaptation of the spec's
  validate(chunk) signature — one method must validate three differently-shaped sources).
  It returns (clean_df, quarantine_df) where quarantine_df is already in the 7-column
  spec schema (helpers.build_quarantine_frame), so write_quarantine just persists + records.
- Unparseable timestamps are treated as a Bronze "invalid data type" validation failure
  (not deferred to Silver) — see README Assumptions for why.
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from etl import helpers, report
from etl.logging_setup import get_logger
from etl.metrics import RunMetrics
from etl.snowflake_loader import NullSnowflakeLoader
from etl.storage import StorageBackend, make_key

SESSION_ID_RE = r"^sess_\d+$"


def _result_row_count(result) -> int:
    """Normalize a SnowflakeLoader.run_sql_file result — a list of fetched rows for
    SELECT-shaped statements, or a bare rowcount int (NullSnowflakeLoader / DML without
    a result set) — into a single row count for the audit log."""
    if isinstance(result, int):
        return result
    try:
        return len(result)
    except TypeError:
        return 0


class WebLogProcessor:
    # (report label, sql/validation/<filename>) — see spec Phase 6 "Post-Load Checks"
    _VALIDATION_QUERIES = (
        ("orphan_user_sk_check", "orphan_user_sk_check.sql"),
        ("duplicate_log_id_check", "duplicate_log_id_check.sql"),
        ("negative_session_duration_check", "negative_session_duration_check.sql"),
    )

    def __init__(
        self,
        weblog_file: str,
        users_file: str,
        products_file: str,
        storage: StorageBackend,
        snowflake_loader,
        settings,
        metrics: RunMetrics | None = None,
    ):
        self.weblog_file = weblog_file
        self.users_file = users_file
        self.products_file = products_file
        self.storage = storage
        self.snowflake_loader = snowflake_loader
        self.settings = settings

        self.etl_run_id = str(uuid.uuid4())
        self.etl_run_date = date.today().isoformat()
        self.etl_run_ts = datetime.now(timezone.utc).isoformat()

        self.metrics = metrics or RunMetrics(self.etl_run_id, self.etl_run_date, self.etl_run_ts)

        # Reference sets for orphan detection — populated by extract() BEFORE weblogs streaming
        self.valid_user_ids: frozenset[int] = frozenset()
        self.valid_product_ids: frozenset[int] = frozenset()
        # Running set threaded across validate() calls to catch cross-chunk log_id dupes
        self._seen_log_ids: set[int] = set()

        self._users_clean: pd.DataFrame | None = None
        self._products_clean: pd.DataFrame | None = None
        self._silver_weblogs: pd.DataFrame | None = None
        self._session_metrics_df: pd.DataFrame | None = None

        self.logger = get_logger(__name__)

    # ── Bronze Phase ──────────────────────────────────────────────────
    def extract(self, chunk_size: int | None = None) -> None:
        """Read source CSVs. Sequencing matters: users/products are small and loaded
        whole FIRST so their clean IDs become the orphan-detection reference sets
        before weblogs — which references them — is streamed in chunks.
        """
        chunk_size = chunk_size or self.settings.chunk_size

        users_raw = pd.read_csv(self.users_file)
        self.metrics.record_extracted("users", len(users_raw))
        clean_users, quarantine_users = self.validate(users_raw, source="users")
        self.write_bronze(clean_users, source="users")
        self.write_quarantine(quarantine_users, source="users")
        self._users_clean = clean_users
        self.valid_user_ids = frozenset(clean_users["user_id"].astype(int))

        products_raw = pd.read_csv(self.products_file)
        self.metrics.record_extracted("products", len(products_raw))
        clean_products, quarantine_products = self.validate(products_raw, source="products")
        self.write_bronze(clean_products, source="products")
        self.write_quarantine(quarantine_products, source="products")
        self._products_clean = clean_products
        self.valid_product_ids = frozenset(clean_products["product_id"].astype(int))

        self.logger.info(
            "extract.reference_sets_ready",
            extra={"valid_user_ids": len(self.valid_user_ids), "valid_product_ids": len(self.valid_product_ids)},
        )

        for chunk_index, chunk in enumerate(pd.read_csv(self.weblog_file, chunksize=chunk_size), start=1):
            self.metrics.record_extracted("weblogs", len(chunk))
            clean_chunk, quarantine_chunk = self.validate(chunk, source="weblogs")
            self.write_bronze(clean_chunk, source="weblogs", chunk_index=chunk_index)
            self.write_quarantine(quarantine_chunk, source="weblogs", chunk_index=chunk_index)
            self.logger.info(
                "extract.chunk_processed",
                extra={"chunk_index": chunk_index, "rows": len(chunk), "clean": len(clean_chunk), "quarantined": len(quarantine_chunk)},
            )

    # ── Validation (dispatches per-source — see module docstring) ─────
    def validate(self, chunk: pd.DataFrame, source: str = "weblogs") -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return (clean_rows, quarantine_rows). quarantine_rows is already shaped to
        the 7-column spec'd quarantine schema (helpers.QUARANTINE_COLUMNS)."""
        if source == "users":
            return self._validate_users(chunk)
        if source == "products":
            return self._validate_products(chunk)
        if source == "weblogs":
            return self._validate_weblogs(chunk)
        raise ValueError(f"Unknown validation source: {source!r}")

    def _quarantine_frame(self, chunk, reject_mask, reasons, source_file):
        return helpers.build_quarantine_frame(
            chunk.loc[reject_mask],
            reasons.loc[reject_mask],
            source_file=source_file,
            etl_run_id=self.etl_run_id,
            etl_run_date=self.etl_run_date,
            etl_run_timestamp=self.etl_run_ts,
        )

    def _validate_users(self, chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        null_user_id = chunk["user_id"].isna()
        dup_user_id = helpers.find_duplicates(chunk["user_id"])
        invalid_email = ~helpers.is_valid_email(chunk["email"])
        invalid_signup_date = helpers.parse_timestamp(chunk["signup_date"]).isna()

        reject_mask = null_user_id | dup_user_id | invalid_email | invalid_signup_date
        reasons = pd.Series(
            np.select(
                [null_user_id, dup_user_id, invalid_email, invalid_signup_date],
                ["null user_id", "duplicate user_id", "invalid email", "invalid signup_date"],
                default=None,
            ),
            index=chunk.index,
        )

        self.metrics.set_quality_observations(
            {"users.null_user_name_rate": float(chunk["user_name"].isna().mean())}
        )

        clean = chunk.loc[~reject_mask].copy()
        quarantine = self._quarantine_frame(chunk, reject_mask, reasons, self.users_file)
        return clean, quarantine

    def _validate_products(self, chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        null_product_id = chunk["product_id"].isna()
        dup_product_id = helpers.find_duplicates(chunk["product_id"])

        reject_mask = null_product_id | dup_product_id
        reasons = pd.Series(
            np.select(
                [null_product_id, dup_product_id],
                ["null product_id", "duplicate product_id"],
                default=None,
            ),
            index=chunk.index,
        )

        self.metrics.set_quality_observations(
            {"products.null_price_rate": float(chunk["price"].isna().mean())}
        )

        clean = chunk.loc[~reject_mask].copy()
        quarantine = self._quarantine_frame(chunk, reject_mask, reasons, self.products_file)
        return clean, quarantine

    def _validate_weblogs(self, chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        null_user_id = chunk["user_id"].isna()

        # Trap (b): cross-chunk duplicate log_id detection via running self._seen_log_ids
        dup_log_id = helpers.find_duplicates(chunk["log_id"], seen=self._seen_log_ids)

        # Trap (a): orphan detection requires self.valid_user_ids/valid_product_ids,
        # which extract() guarantees are populated before weblogs streaming begins.
        user_ids_numeric = pd.to_numeric(chunk["user_id"], errors="coerce")
        orphan_user = user_ids_numeric.notna() & ~user_ids_numeric.isin(self.valid_user_ids)

        product_ids_numeric = pd.to_numeric(chunk["product_id"], errors="coerce")
        orphan_product = product_ids_numeric.notna() & ~product_ids_numeric.isin(self.valid_product_ids)

        session_ids = chunk["session_id"].astype("string")
        invalid_session = session_ids.isna() | ~session_ids.str.match(SESSION_ID_RE, na=False)

        invalid_timestamp = helpers.parse_timestamp(chunk["timestamp"]).isna()

        reject_mask = (
            null_user_id | dup_log_id | orphan_user | orphan_product | invalid_session | invalid_timestamp
        )
        reasons = pd.Series(
            np.select(
                [null_user_id, dup_log_id, orphan_user, orphan_product, invalid_session, invalid_timestamp],
                [
                    "null user_id",
                    "duplicate log_id",
                    "orphan user_id",
                    "orphan product_id",
                    "invalid session_id",
                    "invalid timestamp",
                ],
                default=None,
            ),
            index=chunk.index,
        )

        # Update the running seen-set with KEPT (non-duplicate) ids only — see module docstring.
        kept_log_ids = pd.to_numeric(chunk.loc[~dup_log_id, "log_id"], errors="coerce").dropna().astype(int)
        self._seen_log_ids.update(kept_log_ids.tolist())

        clean = chunk.loc[~reject_mask].copy()
        quarantine = self._quarantine_frame(chunk, reject_mask, reasons, self.weblog_file)
        return clean, quarantine

    # ── Bronze writers ────────────────────────────────────────────────
    def write_bronze(self, clean_chunk: pd.DataFrame, source: str, chunk_index: int | None = None) -> None:
        """Write clean rows to bronze/<source>/ingest_date=<run_date>/ as Parquet."""
        if clean_chunk.empty:
            return
        filename = f"{source}_chunk_{chunk_index:03d}.csv" if chunk_index is not None else f"{source}.csv"
        key = make_key("bronze", source, {"ingest_date": self.etl_run_date}, filename)
        self.storage.write_csv(clean_chunk, key)
        self.metrics.record_loaded("bronze", source, len(clean_chunk))
        self.logger.info("bronze.write", extra={"source": source, "rows": len(clean_chunk), "key": key})

    def write_quarantine(self, quarantine_chunk: pd.DataFrame, source: str, chunk_index: int | None = None) -> None:
        """Write rejected rows (already shaped via build_quarantine_frame) to S3 quarantine.

        `chunk_index` is threaded through for the same reason write_bronze takes one:
        weblogs streams in multiple chunks under the same etl_run_id, and without a
        per-chunk filename each call would overwrite the previous chunk's anomalies
        at the same quarantine key (see helpers.quarantine_path)."""
        if quarantine_chunk.empty:
            return
        key = helpers.quarantine_path(source, self.etl_run_date, self.etl_run_id, chunk_index=chunk_index)
        path = self.storage.write_csv(quarantine_chunk, key)
        self.metrics.record_quarantined(source, len(quarantine_chunk), quarantine_chunk["rejection_reason"])
        self.metrics.record_quarantine_path(source, path)
        self.logger.warning(
            "quarantine.write",
            extra={"source": source, "rows": len(quarantine_chunk), "key": key},
        )

    # ── Silver Phase ──────────────────────────────────────────────────
    def transform(self) -> None:
        """Apply Silver business-logic transformations to Bronze weblogs.

        Bronze is RE-READ via storage (not carried over from extract()'s in-memory
        chunks) so Silver stays an independently re-runnable layer — true to the
        Medallion architecture's intent.

        Per spec Phase 2 "Core Transformations": parse timestamps -> action_ts,
        categorize actions, handle out-of-order logs (sort per session), and compute
        vectorized session metrics. Session-metric *computation* belongs here (it's
        pure pandas/NumPy business logic, as the spec lists it under Silver); its
        *persistence* as AGG_SESSION_METRICS happens in build_gold() — matching where
        the spec's DDL places that table (ANALYTICS schema). See README Assumptions.
        """
        prefix = make_key("bronze", "weblogs", {"ingest_date": self.etl_run_date}, "").rstrip("/")
        weblogs = self.storage.read_csv_prefix(prefix)

        weblogs = weblogs.copy()
        weblogs["action_ts"] = helpers.parse_timestamp(weblogs["timestamp"])
        weblogs["action"] = helpers.categorize_action(weblogs["action"])
        weblogs = helpers.sort_by_timestamp_per_session(weblogs)

        # Embed run metadata directly in the data (mirrors build_quarantine_frame's
        # pattern) so Gold-layer SQL can resolve FACT_USER_ACTIVITY's etl_run_id/
        # etl_run_date and AGG_SESSION_METRICS's etl_run_date without parsing them
        # back out of the S3 partition path.
        weblogs["etl_run_id"] = self.etl_run_id
        weblogs["etl_run_date"] = self.etl_run_date

        self._silver_weblogs = weblogs
        self._session_metrics_df = helpers.compute_session_metrics(weblogs)

        self.metrics.record_session_anomaly(
            "abandoned_cart", int(self._session_metrics_df["is_abandoned_cart"].sum())
        )
        self.metrics.record_session_anomaly(
            "high_activity", int(self._session_metrics_df["is_high_activity"].sum())
        )
        long_sessions = helpers.flag_long_sessions(self._session_metrics_df["session_duration_s"])
        self.metrics.record_session_anomaly("long_session", int(long_sessions.sum()))

        self.logger.info(
            "transform.complete",
            extra={"weblogs_rows": len(weblogs), "sessions": len(self._session_metrics_df)},
        )

    def enrich(self) -> None:
        """Join transformed weblogs with cleaned users/products (spec: 'Merge logs
        with users and products for enrichment'). Re-reads Bronze users/products via
        storage for the same independent-re-runnability reason as transform()."""
        users = self.storage.read_csv(
            make_key("bronze", "users", {"ingest_date": self.etl_run_date}, "users.csv")
        )
        products = self.storage.read_csv(
            make_key("bronze", "products", {"ingest_date": self.etl_run_date}, "products.csv")
        )

        self._silver_weblogs = (
            self._silver_weblogs
            .merge(users[["user_id", "user_name", "email"]], on="user_id", how="left")
            .merge(products[["product_id", "product_name", "category", "price"]], on="product_id", how="left")
        )
        self._users_clean = users
        self._products_clean = products

        self.logger.info("enrich.complete", extra={"weblogs_rows": len(self._silver_weblogs)})

    def write_silver(self, df: pd.DataFrame, dataset: str) -> None:
        """Write a Silver dataset to S3 as Parquet, per the spec's Silver S3 Layout —
        weblogs_clean is partitioned by both etl_run_date and etl_run_id (one file per
        run); users_clean/products_clean are small reference snapshots partitioned only
        by etl_run_date (overwritten on same-day re-runs). A small, deliberate adaptation
        of the spec's write_silver(df) signature — one method must place three differently
        -partitioned datasets; see README Assumptions (mirrors the write_bronze `source` pattern).
        """
        if df.empty:
            return
        if dataset == "weblogs_clean":
            partitions = {"etl_run_date": self.etl_run_date, "etl_run_id": self.etl_run_id}
            filename = "weblogs_silver.csv"
        elif dataset in ("users_clean", "products_clean"):
            partitions = {"etl_run_date": self.etl_run_date}
            filename = f"{dataset.split('_')[0]}_silver.csv"
        else:
            raise ValueError(f"Unknown silver dataset: {dataset!r}")

        key = make_key("silver", dataset, partitions, filename)
        self.storage.write_csv(df, key)
        self.metrics.record_loaded("silver", dataset, len(df))
        self.logger.info("silver.write", extra={"dataset": dataset, "rows": len(df), "key": key})

    # ── Gold Phase ────────────────────────────────────────────────────
    def build_gold(self) -> None:
        """Run Gold-layer SQL in Snowflake (spec Phase 4): idempotent dimension
        upserts (MERGE INTO DIM_USER/DIM_PRODUCT — see sql/dml/merge_dim_*.sql), then
        derive FACT_USER_ACTIVITY and AGG_SESSION_METRICS from STAGING. Pure
        in-warehouse SQL — CREATE TABLE AS SELECT / INSERT, no S3 stage involved."""
        sql_dir = Path(self.settings.sql_dir) / "dml"

        n_users = self.snowflake_loader.merge_dim_user(sql_dir / "merge_dim_user.sql")
        n_products = self.snowflake_loader.merge_dim_product(sql_dir / "merge_dim_product.sql")
        self.metrics.record_loaded("gold", "DIM_USER", n_users)
        self.metrics.record_loaded("gold", "DIM_PRODUCT", n_products)

        n_fact = _result_row_count(self.snowflake_loader.run_sql_file(sql_dir / "load_fact_user_activity.sql"))
        self.metrics.record_loaded("gold", "FACT_USER_ACTIVITY", n_fact)

        n_agg = _result_row_count(self.snowflake_loader.run_sql_file(sql_dir / "build_agg_session_metrics.sql"))
        self.metrics.record_loaded("gold", "AGG_SESSION_METRICS", n_agg)

        self.logger.info(
            "build_gold.complete",
            extra={
                "dim_user": n_users,
                "dim_product": n_products,
                "fact_user_activity": n_fact,
                "agg_session_metrics": n_agg,
            },
        )

    # ── Auditing & Validation (Phase 6) ───────────────────────────────
    def run_validations(self) -> list[dict]:
        """Run the spec's post-load validation queries (orphan FKs, duplicate
        log_ids, negative session durations) against the Gold layer and capture raw
        results for the audit trail / quality report. Meaningless without a live
        warehouse to query, so they're skipped — and clearly marked SKIPPED, never
        silently omitted — when running against NullSnowflakeLoader."""
        if isinstance(self.snowflake_loader, NullSnowflakeLoader):
            self.logger.info("run_validations.skipped_dry_run")
            return [
                {"check": name, "status": "SKIPPED", "result": None, "note": "dry-run / no live Snowflake connection"}
                for name, _ in self._VALIDATION_QUERIES
            ]

        validation_dir = Path(self.settings.sql_dir) / "validation"
        results = []
        for name, filename in self._VALIDATION_QUERIES:
            rows = self.snowflake_loader.run_sql_file(validation_dir / filename)
            results.append({"check": name, "status": "RAN", "result": rows, "note": ""})
            self.logger.info("run_validations.check", extra={"check": name, "result": rows})
        return results

    def _write_audit_log(self) -> None:
        """Insert one ETL_AUDIT_LOG row per Medallion layer (see RunMetrics.to_audit_rows
        for why bronze/silver/gold each get their own row). Best-effort: failures here
        must never mask the pipeline's actual outcome or blow up run()'s finally block,
        so they're logged, not raised."""
        rows = self.metrics.to_audit_rows(source_file=self.weblog_file)
        sql_path = Path(self.settings.sql_dir) / "dml" / "insert_audit_log.sql"
        try:
            sql_text = Path(sql_path).read_text(encoding="utf-8").strip()
            self.snowflake_loader.executemany(sql_text, rows)
            self.logger.info("audit_log.written", extra={"rows": len(rows)})
        except Exception:
            self.logger.exception("audit_log.write_failed")

    # ── Orchestration ─────────────────────────────────────────────────
    def run(self) -> RunMetrics:
        """Orchestrate the full Bronze -> Silver -> Gold pipeline.

        Row-level corruption is handled inside validate() (quarantine and continue —
        per spec, the pipeline must never crash on a bad row). A PHASE-level failure
        (missing file, broken connection, bug) is a different matter: it must still
        produce an auditable FAILED run record rather than crash silently, so it's
        caught here, finalized into the metrics, logged, and re-raised — the audit
        log and quality report are written in `finally` either way (best-effort, see
        _write_audit_log), and the caller (CLI) decides how to report the failure.
        """
        try:
            self.extract()
            self.transform()
            self.enrich()

            self.write_silver(self._silver_weblogs, dataset="weblogs_clean")
            self.write_silver(self._users_clean, dataset="users_clean")
            self.write_silver(self._products_clean, dataset="products_clean")

            self.build_gold()

            self.metrics.set_validation_results(self.run_validations())
            self.metrics.finalize("SUCCESS")
        except Exception as exc:
            self.logger.exception("run.failed")
            self.metrics.finalize("FAILED", error=str(exc))
            raise
        finally:
            self._write_audit_log()
            root_path, history_path = report.write_report(self.metrics, settings=self.settings)
            self.logger.info("run.report_written", extra={"root_path": root_path, "history_path": history_path})

        return self.metrics
