# CLAUDE.md — Capstone Project Team 1: E-Commerce Web Log ETL Pipeline

This file orients Claude Code (and future contributors) to the project. The full spec lives in
[`Capstone_Project_Team_1.md`](./Capstone_Project_Team_1.md) — treat it as the source of truth;
this file summarizes it into actionable tasks and tracks progress.

## Project Overview

Build a **Python-based ETL pipeline** that analyzes e-commerce user behavior from web logs,
following the **Medallion Architecture** (Bronze → Silver → Gold):

- Raw CSVs (`weblogs.csv`, `users.csv`, `products.csv`) are parsed in chunks, validated, and
  landed untransformed in **S3 Bronze** (mirrored into Snowflake `RAW` schema)
- Data is cleaned, deduplicated, enriched, and joined in **S3 Silver** (mirrored into Snowflake
  `STAGING` schema); rows that fail validation are quarantined immediately (never mixed into
  clean data)
- Business-ready fact/dimension tables are built in **Snowflake `ANALYTICS`** (Gold) for BI
  consumption via SQL

**Stack:** Python (Pandas/NumPy), AWS S3 (`boto3`), Snowflake (DDL/DML/`COPY INTO`/`MERGE`)

### Layer ↔ Schema Mapping

| Layer | S3 Prefix | Snowflake Schema | Purpose |
|---|---|---|---|
| Bronze | `s3://bucket/bronze/` | `RAW` | Raw, untransformed, append-only |
| Silver | `s3://bucket/silver/` | `STAGING` | Cleaned, validated, deduped, joined |
| Gold | `s3://bucket/gold/` | `ANALYTICS` | Aggregated fact/dim tables for BI |
| Quarantine | `s3://bucket/quarantine/` | — | Anomalous/rejected rows, partitioned by run |

### Quarantine Pattern (key design decision from spec)

Rejected rows are written to `quarantine/source=<name>/etl_run_date=YYYY-MM-DD/etl_run_id=<uuid>/anomalies.parquet`.
The two-level `etl_run_date` + `etl_run_id` partitioning (rather than a single timestamp) allows
daily prefix scans for ops dashboards while keeping re-runs on the same day from overwriting each
other. Each quarantined row carries: `etl_run_id`, `etl_run_date`, `etl_run_timestamp`,
`source_file`, `row_index`, `rejection_reason`, `raw_row` (JSON).

### Snowflake Database Layout

```
ECOMMERCE_DW
├── RAW          (Bronze mirror): BRONZE_WEBLOGS, BRONZE_USERS, BRONZE_PRODUCTS
├── STAGING      (Silver mirror): WEBLOGS_CLEAN, USERS_CLEAN, PRODUCTS_CLEAN
└── ANALYTICS    (Gold):          DIM_USER, DIM_PRODUCT, FACT_USER_ACTIVITY,
                                  AGG_SESSION_METRICS, ETL_AUDIT_LOG
```

---

## Task Breakdown

### Phase 1 — Extract → Bronze (S3 + Snowflake RAW)
- [ ] Load CSVs in chunks via Pandas (`chunksize=10000`)
- [ ] Validate each chunk's schema (missing columns, invalid types)
- [ ] Handle missing/null user IDs; detect duplicate log entries
- [ ] Detect orphan product IDs and invalid/null session IDs
- [ ] Write anomalous rows to S3 quarantine with full run metadata (see pattern above)
- [ ] Write clean rows to S3 Bronze as Parquet, partitioned by `ingest_date`
- [ ] Create Snowflake external stage (`bronze_stage`) + `COPY INTO RAW.BRONZE_*` from Parquet

### Phase 2 — Transform → Silver (S3 + Snowflake STAGING)
- [ ] Parse timestamps to `datetime`; route unparseable rows to quarantine
- [ ] Categorize actions: `view` / `add_to_cart` / `purchase`
- [ ] Compute session duration via vectorized NumPy (`session_end - session_start`, no loops)
- [ ] Aggregate per-session metrics: total actions, products viewed/purchased
- [ ] Enrich logs by joining with `users` and `products`
- [ ] Compute conversion rate per session (`purchases / views`)
- [ ] Identify abandoned carts (`add_to_cart` with no `purchase`)
- [ ] Flag high-activity sessions (> 50 actions)
- [ ] Sort out-of-order logs by timestamp per session
- [ ] Write enriched data to S3 Silver as Parquet; `COPY INTO STAGING.WEBLOGS_CLEAN`

### Phase 3 — Python Engineering
- [ ] Functional helpers: schema validation, missing-value handling, deduplication, metric
      calculations, quarantine row writing, timestamp parsing, action categorization,
      S3 quarantine path generation
- [ ] Implement `WebLogProcessor` OOP class with methods: `extract`, `validate`, `write_bronze`,
      `write_quarantine`, `transform`, `enrich`, `write_silver`, `load_to_snowflake`,
      `build_gold`, `run` (see class skeleton in spec lines 220–271)
- [ ] Structured (JSON) logging for ETL steps and errors
- [ ] Graceful handling of corrupt rows — quarantine and continue, never crash
- [ ] Track ETL metrics: rows extracted / quarantined / loaded per layer
- [ ] Unit tests for transformations and quarantine logic

### Phase 4 — Load → Gold (Snowflake ANALYTICS)
- [ ] Write DDL for `ECOMMERCE_DW` database, `RAW`/`STAGING`/`ANALYTICS` schemas
- [ ] Create `DIM_USER`, `DIM_PRODUCT` dimension tables
- [ ] Create `FACT_USER_ACTIVITY` fact table, `CLUSTER BY (etl_run_date, action)`
- [ ] Create `AGG_SESSION_METRICS` aggregate table, `CLUSTER BY (etl_run_date)`
- [ ] Implement `MERGE INTO` upsert logic for idempotent dimension loads (`DIM_USER` example
      in spec lines 371–382; replicate pattern for `DIM_PRODUCT`)

### Phase 5 — SQL Analytics (BI Queries)
All ten queries run against `ANALYTICS` schema (full SQL in spec lines 392–457):
- [ ] 1. Most viewed products
- [ ] 2. Overall conversion rate (sessions with purchase / sessions with activity)
- [ ] 3. Average session duration
- [ ] 4. Top users by total purchases
- [ ] 5. Abandoned cart rate per day
- [ ] 6. Product conversion rate (purchases / views per product)
- [ ] 7. Average actions per session
- [ ] 8. Peak activity hours
- [ ] 9. Unusually long sessions (> 2 std deviations above mean)
- [ ] 10. Cohort analysis (signup month vs. purchases)

### Phase 6 — Data Quality & Auditing
- [ ] Create `ETL_AUDIT_LOG` table (DDL in spec lines 466–480)
- [ ] Python: insert audit record per run (rows extracted/transformed/loaded per layer, rows
      quarantined + S3 path, session anomalies count, run status SUCCESS/PARTIAL/FAILED)
- [ ] Post-load validation queries: orphan `user_sk` check, duplicate `log_id` check,
      negative `session_duration_s` check (spec lines 493–508)

### Phase 7 — Performance & Optimization
- [ ] Use vectorized NumPy/Pandas operations throughout — no explicit Python loops
- [ ] Write Parquet (not CSV) to S3 for smaller size and faster `COPY INTO`
- [ ] Multi-threaded S3 uploads via `boto3` `TransferConfig`
- [ ] Use `CLUSTER BY` on fact tables (Snowflake has no traditional indexes)
- [ ] Design deterministic queries to benefit from Snowflake `RESULT_CACHE`
- [ ] Use `EXPLAIN` to analyze query plans; size warehouses appropriately (scale up for Gold
      aggregation, scale down to X-SMALL for BI queries)
- [ ] Consider materialized views for expensive BI queries (example in spec lines 531–537)

### Sample Data Generation (run once, locally)
- [ ] Run the Faker-based generator script (spec lines 593–655, requires
      `pip install pandas numpy faker`) to produce `users.csv` (~1,100 rows), `products.csv`
      (~1,100 rows), and `weblogs.csv` (~15,000 rows). These intentionally contain duplicates,
      nulls, invalid emails/dates/timestamps, and orphan IDs for the pipeline to handle.

### Deliverables Checklist (from spec)
- [ ] `etl/` — Python ETL code (`WebLogProcessor` class + helpers + unit tests)
- [ ] `sql/ddl/` — Snowflake DDL scripts (schemas, tables, stages, clustering)
- [ ] `sql/dml/` — MERGE/upsert scripts + audit log inserts
- [ ] `sql/analytics/` — All 10 BI queries (Phase 5)
- [ ] `sql/validation/` — Post-load data quality checks (Phase 6)
- [ ] `data_quality_report.md` — Generated by the ETL pipeline per run
- [ ] `diagrams/erd.png` — ERD for Gold layer
- [ ] `diagrams/architecture.png` — Medallion + S3 + Snowflake flow diagram
- [ ] `README.md` — Architecture decisions, Snowflake setup, assumptions

---

## Known Issues

### `AGG_SESSION_METRICS` row count exceeds distinct session count (grain mismatch)

**Symptom:** After a clean run, `ANALYTICS.AGG_SESSION_METRICS` has more rows than distinct
`session_id`s reported by `transform.complete` in the pipeline logs (e.g. 8,673 rows for 4,108
distinct sessions on the 2026-06-11 run). `SELECT session_id, COUNT(*) ... HAVING COUNT(*) > 1`
shows the same `session_id` appearing 2-5+ times.

**Root cause:** Two different aggregation grains are used for "session metrics":
- Python (`etl/helpers.compute_session_metrics`, used for `transform.complete` logging and the
  data quality report) groups by `session_id` alone.
- The Gold-layer SQL (`sql/dml/build_agg_session_metrics.sql`) groups by
  `(session_id, user_sk, etl_run_date)` after joining `STAGING`/silver weblogs to `DIM_USER`.

In the Faker-generated sample data, `session_id` values are not guaranteed unique per user — the
same `session_id` string can appear across multiple different `user_id`s. The SQL grain
therefore "fans out" one logical session into multiple `AGG_SESSION_METRICS` rows (one per
distinct user sharing that `session_id`), while the Python-side count treats it as one session.

**Why it isn't caught by existing checks:** none of the Phase 6 validation queries
(`null_sk_check`, `orphan_user_sk_check`, `duplicate_log_id_check`,
`negative_session_duration_check`) check for this — they operate on `FACT_USER_ACTIVITY` (which
is correct, grain = `log_id`) or check for true SQL-level duplicate `session_id` rows with
identical `(session_id, user_sk)`, which this isn't.

**Possible fixes (pick one when addressing):**
1. **Make `session_id` generation/validation guarantee uniqueness** — e.g. during Bronze
   validation (Phase 1), treat a `session_id` reused across different `user_id`s as an anomaly
   and quarantine/regenerate it as `<user_id>_<session_id>`. Cleanest long-term fix; matches the
   spec's intent that a session belongs to one user.
2. **Change `build_agg_session_metrics.sql`'s grain to `session_id` only** — drop `user_sk` from
   `GROUP BY` and pick a representative `user_sk` (e.g. `MIN(u.user_sk)` or `ANY_VALUE`) per
   session, matching the Python grain. Simpler, but masks the underlying multi-user-per-session
   data quality issue rather than fixing it.
3. **Add a new Phase 6 validation** comparing `COUNT(DISTINCT session_id)` from
   `AGG_SESSION_METRICS` against the Python-computed session count in the audit log /
   data quality report, so future runs surface this discrepancy explicitly instead of relying on
   manual verification.

Found during a clean S3 + Snowflake wipe & full pipeline re-run on 2026-06-11 (run_id
`cb4a8d30-f32c-4ecb-9925-230e5e37c136`); not a regression from that run — present in the SQL/data
design as written.

---

## Resolved Issues

### `ETL_AUDIT_LOG` gold-layer `rows_loaded` showed `4` instead of the real row count (FIXED 2026-06-11)

**Symptom:** After a run, `ANALYTICS.ETL_AUDIT_LOG`'s `gold` row reported `ROWS_LOADED = 4`,
even though `DIM_USER` (1,019), `DIM_PRODUCT` (1,079), `FACT_USER_ACTIVITY` (9,402), and
`AGG_SESSION_METRICS` (9,393) were all populated correctly.

**Root cause:** `_result_row_count` (`etl/processor.py`) and `_affected_rows`
(`etl/snowflake_loader.py`) computed `len(result)` on the result of each gold-layer
`MERGE`/`INSERT ... SELECT` statement. Snowflake's `cursor.fetchall()` returns exactly **one
row** for these statements, whose *values* are the affected-row counts (e.g. `[(1019, 0)]` for
a MERGE — rows inserted, rows updated; `[(9402,)]` for an INSERT). `len(result)` is therefore
always `1`, so `record_loaded("gold", ...)` was called four times with `1`, summing to `4`.

**Fix:** Both helpers now `sum()` the numeric values inside the returned row(s) instead of
counting rows. Verified against a live Snowflake run: `dim_user: 1019, dim_product: 1079`
(first run after a clean wipe — both MERGEs were pure inserts), and on a same-day re-run
`fact_user_activity: 0, agg_session_metrics: 0` (correct — the `NOT EXISTS` idempotency guards
in `load_fact_user_activity.sql` / `build_agg_session_metrics.sql` skip already-loaded rows).

---

## Progress Report

**Status as of 2026-06-11: Pipeline implemented end-to-end and runs successfully against real
S3 + Snowflake (clean wipe + full re-run completed). Recent work: updated Bronze validation
business rules for users/products (dedupe-keep-latest, flag instead of reject for
duplicate/invalid emails, fill missing `user_name`/`price`), added the previously-missing
`sql/ddl/02_raw_tables.sql` / `03_staging_tables.sql`, and fixed the `ETL_AUDIT_LOG` gold
`rows_loaded` undercount (see Resolved Issues above).**

| Area | Status | Notes |
|---|---|---|
| Sample data generation | Done | `data/raw/{users,products,weblogs}.csv` generated |
| Phase 1 — Extract → Bronze | Done | `_validate_users`/`_validate_products`/`_validate_weblogs` in `etl/processor.py`; users/products business rules updated 2026-06-11 |
| Phase 2 — Transform → Silver | Done | `transform`/`enrich` in `etl/processor.py` + `etl/helpers.py` |
| Phase 3 — Python Engineering (`WebLogProcessor`, helpers, tests) | Done | `etl/processor.py`, `etl/helpers.py`, `tests/` (some pre-existing test failures remain — parquet/csv path mismatches in a few `write_bronze` tests, unrelated to recent changes) |
| Phase 4 — Load → Gold (DDL + MERGE upserts) | Done | `sql/ddl/04_analytics_tables.sql`, `sql/dml/merge_dim_*.sql`, `load_fact_user_activity.sql`, `build_agg_session_metrics.sql`; `RAW`/`STAGING` table DDL added 2026-06-11 (currently created but not populated — no `COPY INTO` step yet) |
| Phase 5 — SQL Analytics (10 BI queries) | Done | `sql/analytics/01..10_*.sql` |
| Phase 6 — Data Quality & Auditing | Done | `sql/ddl/05_audit_table.sql`, `sql/dml/insert_audit_log.sql`, `sql/validation/*.sql`; gold `rows_loaded` audit bug fixed 2026-06-11; `AGG_SESSION_METRICS` grain-mismatch issue still open (see Known Issues) |
| Phase 7 — Performance & Optimization | Mostly Done | Vectorized pandas/NumPy throughout, `CLUSTER BY` on fact/agg tables, multi-threaded S3 uploads via `etl/storage.py`; Bronze/Silver still written as CSV (spec calls for Parquet) |
| Deliverables (`etl/`, `sql/`, diagrams, README, data quality report) | Mostly Done | `etl/`, `sql/`, `diagrams/`, `data_quality_report.md` + `run_reports/` present; top-level `README.md` not yet written |

> Update this table as work progresses — change status to **In Progress** / **Done**, and add
> dated notes for decisions, blockers, or deviations from the spec.
