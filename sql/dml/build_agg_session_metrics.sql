-- Derive AGG_SESSION_METRICS directly from STAGING.WEBLOGS_CLEAN (joined to DIM_USER
-- for user_sk) via a single GROUP BY — a self-contained, idiomatic Gold-layer
-- aggregation (spec build_gold: "CREATE TABLE AS SELECT"). The session-metric
-- *formulas* mirror etl/helpers.compute_session_metrics exactly (same definitions
-- of duration, conversion_rate, is_abandoned_cart, is_high_activity), so Python's
-- data_quality_report and this Gold table always agree — see README Assumptions for
-- why this is computed in SQL here rather than COPY INTO'd from the Python DataFrame
-- (user_sk is a Snowflake-side surrogate key that doesn't exist until merge_dim_user
-- has run, so it can only be resolved with a live join).
-- Reads from STAGING.WEBLOGS_CLEAN (loaded via COPY INTO ... MATCH_BY_COLUMN_NAME from
-- Silver Parquet) rather than re-parsing raw stage files, so action_ts is a properly
-- typed TIMESTAMP_NTZ and MIN/MAX produce real session start/end values.
-- Idempotent: re-runs skip session_ids already present.
INSERT INTO ANALYTICS.AGG_SESSION_METRICS
    (session_id, user_sk, session_start, session_end, session_duration_s,
     total_actions, total_views, total_cart_adds, total_purchases,
     conversion_rate, is_abandoned_cart, is_high_activity, etl_run_date)
SELECT
    w.session_id,
    u.user_sk,
    MIN(w.action_ts)                                                AS session_start,
    MAX(w.action_ts)                                                AS session_end,
    DATEDIFF('second', MIN(w.action_ts), MAX(w.action_ts))::FLOAT   AS session_duration_s,
    COUNT(*)                                                        AS total_actions,
    SUM(IFF(w.action = 'view', 1, 0))                               AS total_views,
    SUM(IFF(w.action = 'add_to_cart', 1, 0))                        AS total_cart_adds,
    SUM(IFF(w.action = 'purchase', 1, 0))                           AS total_purchases,
    SUM(IFF(w.action = 'purchase', 1, 0))::FLOAT
        / NULLIF(SUM(IFF(w.action = 'view', 1, 0)), 0)              AS conversion_rate,
    (SUM(IFF(w.action = 'add_to_cart', 1, 0)) > 0
        AND SUM(IFF(w.action = 'purchase', 1, 0)) = 0)              AS is_abandoned_cart,
    (COUNT(*) > 50)                                                  AS is_high_activity,
    MAX(w.etl_run_date)                                              AS etl_run_date
FROM STAGING.WEBLOGS_CLEAN AS w
JOIN ANALYTICS.DIM_USER AS u ON w.user_id = u.user_id
WHERE NOT EXISTS (
    SELECT 1 FROM ANALYTICS.AGG_SESSION_METRICS AS a WHERE a.session_id = w.session_id
)
GROUP BY w.session_id, u.user_sk;
