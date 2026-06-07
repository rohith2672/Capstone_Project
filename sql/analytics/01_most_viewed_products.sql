-- 1. Most viewed products — spec Phase 5, verbatim.
SELECT p.product_name, COUNT(*) AS view_count
FROM ANALYTICS.FACT_USER_ACTIVITY f
JOIN ANALYTICS.DIM_PRODUCT p ON f.product_sk = p.product_sk
WHERE f.action = 'view'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
