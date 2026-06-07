-- 8. Peak activity hours — spec Phase 5, verbatim.
SELECT HOUR(action_ts) AS hour_of_day, COUNT(*) AS action_count
FROM ANALYTICS.FACT_USER_ACTIVITY
GROUP BY 1 ORDER BY 1;
