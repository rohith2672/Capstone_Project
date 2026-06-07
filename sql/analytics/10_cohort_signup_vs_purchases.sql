-- 10. Cohort analysis: users by signup month vs purchases — spec Phase 5, verbatim.
SELECT
    DATE_TRUNC('month', u.signup_date) AS signup_cohort,
    SUM(s.total_purchases) AS total_purchases,
    COUNT(DISTINCT s.user_sk) AS active_users
FROM ANALYTICS.AGG_SESSION_METRICS s
JOIN ANALYTICS.DIM_USER u ON s.user_sk = u.user_sk
GROUP BY 1 ORDER BY 1;
