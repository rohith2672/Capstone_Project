-- Idempotent dimension upsert (spec Phase 4 "Upsert Logic", verbatim).
-- Run after STAGING.USERS_CLEAN has been refreshed via COPY INTO.
MERGE INTO ANALYTICS.DIM_USER AS target
USING (
    SELECT 
        $1::string AS user_id,
        $2::string AS user_name,
        $3::string AS email,
        $4::timestamp AS signup_date
    FROM @ANALYTICS.silver_stage/users_clean/
    (FILE_FORMAT => 'ANALYTICS.csv_format', PATTERN => '.*\\.csv')
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
