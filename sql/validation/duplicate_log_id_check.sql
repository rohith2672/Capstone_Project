-- No duplicate log entries in fact table — spec Phase 6, verbatim.
-- Expect: zero rows returned
SELECT log_id, COUNT(*) AS cnt
FROM ANALYTICS.FACT_USER_ACTIVITY
GROUP BY 1 HAVING cnt > 1;
