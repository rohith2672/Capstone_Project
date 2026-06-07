-- 2. Overall conversion rate (sessions with purchase / sessions with any activity) — spec Phase 5, verbatim.
SELECT
    COUNT(DISTINCT CASE WHEN total_purchases > 0 THEN session_id END)::FLOAT
    / NULLIF(COUNT(DISTINCT session_id), 0) AS overall_conversion_rate
FROM ANALYTICS.AGG_SESSION_METRICS;
