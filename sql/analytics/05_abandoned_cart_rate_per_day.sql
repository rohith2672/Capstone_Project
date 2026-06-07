-- 5. Abandoned cart rate per day — spec Phase 5, verbatim.
SELECT etl_run_date,
    SUM(CASE WHEN is_abandoned_cart THEN 1 ELSE 0 END)::FLOAT
    / NULLIF(COUNT(*), 0) AS abandoned_cart_rate
FROM ANALYTICS.AGG_SESSION_METRICS
GROUP BY 1 ORDER BY 1;
