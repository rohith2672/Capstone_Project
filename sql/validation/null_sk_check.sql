-- Null surrogate keys in FACT_USER_ACTIVITY (spec Phase 6 data-quality checks).
-- Expect: null_user_sk = 0 AND null_product_sk = 0
SELECT
    SUM(IFF(user_sk IS NULL, 1, 0))    AS null_user_sk,
    SUM(IFF(product_sk IS NULL, 1, 0)) AS null_product_sk
FROM ANALYTICS.FACT_USER_ACTIVITY;
