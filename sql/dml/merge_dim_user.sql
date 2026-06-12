-- Idempotent dimension upsert (spec Phase 4 "Upsert Logic", verbatim).
-- Reads from STAGING.USERS_CLEAN (loaded via COPY INTO ... MATCH_BY_COLUMN_NAME from
-- Silver Parquet) rather than re-parsing raw stage files, so columns are properly typed.
-- Run after STAGING.USERS_CLEAN has been refreshed via COPY INTO.
MERGE INTO ANALYTICS.DIM_USER AS target
USING (
    SELECT
        user_id,
        user_name,
        email,
        signup_date::date AS signup_date
    FROM STAGING.USERS_CLEAN
) AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN UPDATE SET
    user_name     = source.user_name,
    email         = source.email,
    dw_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (user_id, user_name, email, signup_date)
VALUES
    (source.user_id, source.user_name, source.email, source.signup_date);
