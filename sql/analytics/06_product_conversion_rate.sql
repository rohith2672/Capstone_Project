-- 6. Product conversion rate (purchases / views per product) — spec Phase 5, verbatim.
SELECT p.product_name,
    SUM(CASE WHEN f.action='purchase' THEN 1 ELSE 0 END)::FLOAT
    / NULLIF(SUM(CASE WHEN f.action='view' THEN 1 ELSE 0 END), 0) AS product_cvr
FROM ANALYTICS.FACT_USER_ACTIVITY f
JOIN ANALYTICS.DIM_PRODUCT p ON f.product_sk = p.product_sk
GROUP BY 1 ORDER BY 2 DESC NULLS LAST;
