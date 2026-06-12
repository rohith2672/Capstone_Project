-- Derive FACT_USER_ACTIVITY from STAGING.WEBLOGS_CLEAN, resolving DIM_USER/DIM_PRODUCT
-- surrogate keys via join (the spec's build_gold "Run Gold-layer SQL ... CREATE TABLE
-- AS SELECT" — written here as an idempotent INSERT...SELECT so re-runs on the same
-- day don't duplicate rows; log_id is the natural key for the NOT EXISTS guard).
-- Reads from STAGING.WEBLOGS_CLEAN (loaded via COPY INTO ... MATCH_BY_COLUMN_NAME from
-- Silver Parquet) rather than re-parsing raw stage files, so action_ts/etl_run_id/
-- etl_run_date are properly typed columns.
-- Run AFTER merge_dim_user.sql / merge_dim_product.sql so surrogate keys exist.
INSERT INTO ANALYTICS.FACT_USER_ACTIVITY
    (log_id, user_sk, product_sk, session_id, action, action_ts, etl_run_id, etl_run_date)
SELECT
    w.log_id,
    u.user_sk,
    p.product_sk,
    w.session_id,
    w.action,
    w.action_ts,
    w.etl_run_id,
    w.etl_run_date
FROM STAGING.WEBLOGS_CLEAN AS w
LEFT JOIN ANALYTICS.DIM_USER AS u ON w.user_id = u.user_id
LEFT JOIN ANALYTICS.DIM_PRODUCT AS p ON w.product_id = p.product_id
WHERE NOT EXISTS (
    SELECT 1 FROM ANALYTICS.FACT_USER_ACTIVITY AS f WHERE f.log_id = w.log_id
);
