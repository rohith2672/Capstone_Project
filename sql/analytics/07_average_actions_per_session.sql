-- 7. Average actions per session — spec Phase 5, verbatim.
SELECT AVG(total_actions) AS avg_actions_per_session
FROM ANALYTICS.AGG_SESSION_METRICS;
