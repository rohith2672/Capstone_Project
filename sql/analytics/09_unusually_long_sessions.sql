-- 9. Unusually long sessions (> 2 std deviations above mean) — spec Phase 5, verbatim.
SELECT session_id, session_duration_s
FROM ANALYTICS.AGG_SESSION_METRICS
WHERE session_duration_s > (
    SELECT AVG(session_duration_s) + 2 * STDDEV(session_duration_s)
    FROM ANALYTICS.AGG_SESSION_METRICS
);
