-- 4. Top users by total purchases — spec Phase 5, verbatim.
SELECT u.user_name, SUM(s.total_purchases) AS total_purchases
FROM ANALYTICS.AGG_SESSION_METRICS s
JOIN ANALYTICS.DIM_USER u ON s.user_sk = u.user_sk
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
