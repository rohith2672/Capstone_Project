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

## Progress Report

**Status as of 2026-06-07: Project initialized — spec received, git repo created, no
implementation started yet.**

**2026-06-12: Fixed `session_duration_s = 0` bug in `ANALYTICS.AGG_SESSION_METRICS`.**
Root cause: the Gold-layer DML (`build_agg_session_metrics.sql`,
`load_fact_user_activity.sql`, `merge_dim_user.sql`, `merge_dim_product.sql`) re-parsed
raw `silver_stage` files as CSV with positional `$N` references, but `pipeline.py`
writes Parquet to that prefix with a different column layout — so `action_ts` resolved
to `NULL` for every row and `MIN(action_ts) = MAX(action_ts)`. Fixed by:
- `etl/pipeline.py` `transform()` now renames `timestamp` → `action_ts` and stamps
  `etl_run_id`/`etl_run_date` on the Silver weblogs DataFrame.
- Added missing `sql/ddl/02_raw_tables.sql` and `sql/ddl/03_staging_tables.sql`
  (RAW/STAGING schemas + tables were never created, so `COPY INTO STAGING.WEBLOGS_CLEAN`
  had no target).
- All four Gold DML scripts now read from `STAGING.WEBLOGS_CLEAN` /
  `STAGING.USERS_CLEAN` / `STAGING.PRODUCTS_CLEAN` (populated via
  `COPY INTO ... MATCH_BY_COLUMN_NAME` from Silver Parquet) instead of re-parsing stage
  files.

| Area | Status | Notes |
|---|---|---|
| Sample data generation | Not Started | Run generator script to produce input CSVs |
| Phase 1 — Extract → Bronze | Not Started | |
| Phase 2 — Transform → Silver | Not Started | |
| Phase 3 — Python Engineering (`WebLogProcessor`, helpers, tests) | Not Started | |
| Phase 4 — Load → Gold (DDL + MERGE upserts) | In Progress | RAW/STAGING/ANALYTICS DDL + MERGE/INSERT scripts now read from STAGING tables (see 2026-06-12 note) |
| Phase 5 — SQL Analytics (10 BI queries) | Not Started | |
| Phase 6 — Data Quality & Auditing | Not Started | |
| Phase 7 — Performance & Optimization | Not Started | |
| Deliverables (`etl/`, `sql/`, diagrams, README, data quality report) | Not Started | |

> Update this table as work progresses — change status to **In Progress** / **Done**, and add
> dated notes for decisions, blockers, or deviations from the spec.
