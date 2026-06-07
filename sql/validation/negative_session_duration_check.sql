-- All session durations are non-negative — spec Phase 6, verbatim.
-- Expect: invalid_duration_count = 0
SELECT COUNT(*) AS invalid_duration_count
FROM ANALYTICS.AGG_SESSION_METRICS
WHERE session_duration_s < 0;
