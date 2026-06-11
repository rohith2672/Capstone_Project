# Data Quality Report — Run `4e270749-3723-46a8-8dfd-80dfcc11527d`

- **Run date:** 2026-06-11
- **Run timestamp:** 2026-06-11T16:21:29.039895+00:00
- **Status:** SUCCESS

## Row Counts

### Extraction & Quarantine by Source

| Source | Extracted | Quarantined |
|---|---:|---:|
| products | 1100 | 0 |
| users | 1100 | 57 |
| weblogs | 15000 | 5598 |

### Rows Loaded by Layer

| Layer | Dataset | Rows Loaded |
|---|---|---:|
| bronze | users | 1019 |
| bronze | products | 1079 |
| bronze | weblogs | 9402 |
| silver | weblogs_clean | 9402 |
| silver | users_clean | 1019 |
| silver | products_clean | 1079 |
| gold | DIM_USER | 1019 |
| gold | DIM_PRODUCT | 1079 |
| gold | FACT_USER_ACTIVITY | 0 |
| gold | AGG_SESSION_METRICS | 0 |

## Quarantine Breakdown

| Source | Rejection Reason | Count |
|---|---|---:|
| users | invalid signup_date | 57 |
| weblogs | orphan user_id | 2170 |
| weblogs | orphan product_id | 1154 |
| weblogs | null user_id | 738 |
| weblogs | invalid session_id | 529 |
| weblogs | invalid timestamp | 524 |
| weblogs | duplicate log_id | 483 |

**Quarantine object paths:**
- `users`: `s3://ecommercebucket17/quarantine/source=users/etl_run_date=2026-06-11/etl_run_id=4e270749-3723-46a8-8dfd-80dfcc11527d/anomalies.parquet`
- `weblogs`: `s3://ecommercebucket17/quarantine/source=weblogs/etl_run_date=2026-06-11/etl_run_id=4e270749-3723-46a8-8dfd-80dfcc11527d/anomalies_chunk_001.parquet`
- `weblogs`: `s3://ecommercebucket17/quarantine/source=weblogs/etl_run_date=2026-06-11/etl_run_id=4e270749-3723-46a8-8dfd-80dfcc11527d/anomalies_chunk_002.parquet`

## Data Quality Observations

_None recorded._

## Session-Level Anomalies

| Anomaly | Sessions Flagged |
|---|---:|
| abandoned_cart | 1233 |
| high_activity | 0 |
| long_session | 104 |
| null_sk_in_fact | 0 |

## Post-Load Validation Results

| Check | Status | Detail |
|---|---|---|
| orphan_user_sk_check | RAN | [(0,)] |
| duplicate_log_id_check | RAN | [] |
| negative_session_duration_check | RAN | [(0,)] |
| null_sk_check | RAN | [(0, 0)] |

## Recommendations

- Quarantine rate is **32.9%** of extracted rows — investigate upstream data quality at the source system(s) before the next run.
- **1233** abandoned-cart sessions detected — candidates for retargeting/marketing analysis.
- **104** unusually long sessions flagged (> 2 std. dev. above the mean duration) — review for bot traffic, idle tabs, or instrumentation issues.
