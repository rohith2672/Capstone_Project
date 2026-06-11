-- No orphan session IDs (fact rows with no matching dim) — spec Phase 6, verbatim.
-- Expect: orphan_users = 0
SELECT COUNT(*) AS orphan_users
FROM ANALYTICS.FACT_USER_ACTIVITY
WHERE user_sk IS NULL
   OR user_sk NOT IN (SELECT user_sk FROM ANALYTICS.DIM_USER);
