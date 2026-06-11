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

USE SCHEMA ANALYTICS;

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1;

-- Named stages — one per Medallion S3 prefix
CREATE OR REPLACE STAGE bronze_stage
    URL = 's3://<bucket>/bronze/'
    CREDENTIALS = (AWS_KEY_ID = '<aws_key_id>' AWS_SECRET_KEY = '<aws_secret_key>')
    FILE_FORMAT = csv_format;

CREATE OR REPLACE STAGE silver_stage
    URL = 's3://<bucket>/silver/'
    CREDENTIALS = (AWS_KEY_ID = '<aws_key_id>' AWS_SECRET_KEY = '<aws_secret_key>')
    FILE_FORMAT = csv_format;

CREATE OR REPLACE STAGE quarantine_stage
    URL = 's3://<bucket>/quarantine/'
    CREDENTIALS = (AWS_KEY_ID = '<aws_key_id>' AWS_SECRET_KEY = '<aws_secret_key>')
    FILE_FORMAT = csv_format;
