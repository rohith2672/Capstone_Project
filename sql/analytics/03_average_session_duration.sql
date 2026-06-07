-- 3. Average session duration — spec Phase 5, verbatim.
SELECT AVG(session_duration_s) / 60 AS avg_duration_minutes
FROM ANALYTICS.AGG_SESSION_METRICS
WHERE session_duration_s >= 0;
