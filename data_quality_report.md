# Data Quality Report — Run `8c11a28f-a782-49a5-846d-7e984191a971`

- **Run date:** 2026-06-07
- **Run timestamp:** 2026-06-07T20:17:22.941998+00:00
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
| gold | DIM_USER | 0 |
| gold | DIM_PRODUCT | 0 |
| gold | FACT_USER_ACTIVITY | 0 |
| gold | AGG_SESSION_METRICS | 0 |

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
| weblogs | duplicate log_id | 483 |
| weblogs | invalid session_id | 483 |
| weblogs | invalid timestamp | 477 |

**Quarantine object paths:**
- `users`: `data/lake\quarantine\source=users\etl_run_date=2026-06-07\etl_run_id=8c11a28f-a782-49a5-846d-7e984191a971\anomalies.parquet`
- `products`: `data/lake\quarantine\source=products\etl_run_date=2026-06-07\etl_run_id=8c11a28f-a782-49a5-846d-7e984191a971\anomalies.parquet`
- `weblogs`: `data/lake\quarantine\source=weblogs\etl_run_date=2026-06-07\etl_run_id=8c11a28f-a782-49a5-846d-7e984191a971\anomalies_chunk_001.parquet`
- `weblogs`: `data/lake\quarantine\source=weblogs\etl_run_date=2026-06-07\etl_run_id=8c11a28f-a782-49a5-846d-7e984191a971\anomalies_chunk_002.parquet`
- `weblogs`: `data/lake\quarantine\source=weblogs\etl_run_date=2026-06-07\etl_run_id=8c11a28f-a782-49a5-846d-7e984191a971\anomalies_chunk_003.parquet`

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
| orphan_user_sk_check | SKIPPED | dry-run / no live Snowflake connection |
| duplicate_log_id_check | SKIPPED | dry-run / no live Snowflake connection |
| negative_session_duration_check | SKIPPED | dry-run / no live Snowflake connection |

## Recommendations

- Quarantine rate is **37.8%** of extracted rows — investigate upstream data quality at the source system(s) before the next run.
- **1223** abandoned-cart sessions detected — candidates for retargeting/marketing analysis.
- **136** unusually long sessions flagged (> 2 std. dev. above the mean duration) — review for bot traffic, idle tabs, or instrumentation issues.
