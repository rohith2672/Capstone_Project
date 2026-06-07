-- External stages over the S3 lake — INFERRED / adapted from the spec's
-- "CREATE OR REPLACE STAGE bronze_stage ... CREDENTIALS = (AWS_KEY_ID = '...'
-- AWS_SECRET_KEY = '...')" example (lines ~148-152).
--
-- DELIBERATE DEVIATION: we do NOT inline AWS_KEY_ID/AWS_SECRET_KEY. Static
-- credentials embedded in DDL text are a security anti-pattern — they persist in
-- query history, account usage views, and DDL dumps, and must be rotated by hand.
-- Snowflake's STORAGE INTEGRATION lets the warehouse assume an IAM role via STS
-- instead: zero static keys ever appear in SQL. See README "Architecture Decisions"
-- for the full rationale and the one-time IAM trust-policy setup this requires
-- (DESC INTEGRATION exposes STORAGE_AWS_IAM_USER_ARN / STORAGE_AWS_EXTERNAL_ID to
-- paste into the role's trust policy).

CREATE OR REPLACE STORAGE INTEGRATION s3_ecommerce_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<arn:aws:iam::ACCOUNT_ID:role/snowflake-ecommerce-dw-role>'
    STORAGE_ALLOWED_LOCATIONS = (
        's3://<bucket>/bronze/',
        's3://<bucket>/silver/',
        's3://<bucket>/quarantine/'
    );

-- One-time, after running the above:
--   DESC INTEGRATION s3_ecommerce_integration;
-- copy STORAGE_AWS_IAM_USER_ARN + STORAGE_AWS_EXTERNAL_ID into the IAM role's
-- trust policy in the AWS console, then the stages below can read/list the bucket.

CREATE OR REPLACE FILE FORMAT parquet_format
    TYPE = PARQUET;

-- Named stages — one per Medallion S3 prefix (matches processor.load_to_snowflake's
-- f"{layer}_stage/{dataset}/" addressing and the spec's S3 Bucket Layout).
CREATE OR REPLACE STAGE bronze_stage
    STORAGE_INTEGRATION = s3_ecommerce_integration
    URL = 's3://<bucket>/bronze/'
    FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE silver_stage
    STORAGE_INTEGRATION = s3_ecommerce_integration
    URL = 's3://<bucket>/silver/'
    FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE quarantine_stage
    STORAGE_INTEGRATION = s3_ecommerce_integration
    URL = 's3://<bucket>/quarantine/'
    FILE_FORMAT = parquet_format;

-- Example loads (mirrors spec lines ~155-158 / ~196-198); the actual COPY INTO
-- statements run by the pipeline are built in SnowflakeLoader.copy_into() —
-- these are reference examples for manual/ad-hoc loading & debugging.
--
-- COPY INTO RAW.BRONZE_WEBLOGS
-- FROM @bronze_stage/weblogs/
-- FILE_FORMAT = (TYPE = PARQUET)
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- ON_ERROR = CONTINUE;
--
-- COPY INTO STAGING.WEBLOGS_CLEAN
-- FROM @silver_stage/weblogs_clean/
-- FILE_FORMAT = (TYPE = PARQUET)
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- ON_ERROR = CONTINUE;
