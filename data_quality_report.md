# Data Quality Report — Run `b52754c8-3360-4542-a26a-573759171b92`

- **Run date:** 2026-06-10
- **Run timestamp:** 2026-06-10T21:22:44.631111+00:00
- **Status:** SUCCESS

## Row Counts

### Extraction & Quarantine by Source

| Source | Extracted | Quarantined |
|---|---:|---:|
| products | 1100 | 21 |
| users | 1100 | 168 |
| weblogs | 15000 | 6318 |

### Rows Loaded by Layer

| Layer | Dataset | Rows Loaded |
|---|---|---:|
| bronze | users | 932 |
| bronze | products | 1079 |
| bronze | weblogs | 8682 |
| silver | weblogs_clean | 8682 |
| silver | users_clean | 932 |
| silver | products_clean | 1079 |
| gold | DIM_USER | 1 |
| gold | DIM_PRODUCT | 1 |
| gold | FACT_USER_ACTIVITY | 1 |
| gold | AGG_SESSION_METRICS | 1 |

## Quarantine Breakdown

| Source | Rejection Reason | Count |
|---|---|---:|
| users | invalid email | 95 |
| users | invalid signup_date | 49 |
| users | duplicate user_id | 24 |
| products | duplicate product_id | 21 |
| weblogs | orphan user_id | 3093 |
| weblogs | orphan product_id | 1044 |
| weblogs | null user_id | 738 |
| weblogs | invalid session_id | 483 |
| weblogs | duplicate log_id | 483 |
| weblogs | invalid timestamp | 477 |

**Quarantine object paths:**
- `users`: `s3://ecommercebucket17/quarantine/source=users/etl_run_date=2026-06-10/etl_run_id=b52754c8-3360-4542-a26a-573759171b92/anomalies.parquet`
- `products`: `s3://ecommercebucket17/quarantine/source=products/etl_run_date=2026-06-10/etl_run_id=b52754c8-3360-4542-a26a-573759171b92/anomalies.parquet`
- `weblogs`: `s3://ecommercebucket17/quarantine/source=weblogs/etl_run_date=2026-06-10/etl_run_id=b52754c8-3360-4542-a26a-573759171b92/anomalies_chunk_001.parquet`
- `weblogs`: `s3://ecommercebucket17/quarantine/source=weblogs/etl_run_date=2026-06-10/etl_run_id=b52754c8-3360-4542-a26a-573759171b92/anomalies_chunk_002.parquet`

## Data Quality Observations

| Observation | Rate |
|---|---:|
| products.null_price_rate | 11.00% |
| users.null_user_name_rate | 4.09% |

## Session-Level Anomalies

| Anomaly | Sessions Flagged |
|---|---:|
| abandoned_cart | 1223 |
| high_activity | 0 |
| long_session | 136 |

## Post-Load Validation Results

| Check | Status | Detail |
|---|---|---|
| orphan_user_sk_check | RAN | [(0,)] |
| duplicate_log_id_check | RAN | [] |
| negative_session_duration_check | RAN | [(0,)] |

## Recommendations

- Quarantine rate is **37.8%** of extracted rows — investigate upstream data quality at the source system(s) before the next run.
- **1223** abandoned-cart sessions detected — candidates for retargeting/marketing analysis.
- **136** unusually long sessions flagged (> 2 std. dev. above the mean duration) — review for bot traffic, idle tabs, or instrumentation issues.
