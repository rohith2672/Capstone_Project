-- STAGING schema (Silver mirror) — landing tables for COPY INTO STAGING.*_CLEAN in
-- pipeline.py load_to_snowflake('silver'), loaded from S3 Silver Parquet via
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE. Column names/types match the Silver
-- DataFrames written by pipeline.py transform() (timestamp -> action_ts, plus
-- etl_run_id/etl_run_date stamped on every row), so Gold-layer DML can read directly
-- from these tables instead of re-parsing raw stage files.

CREATE SCHEMA IF NOT EXISTS STAGING;

CREATE OR REPLACE TABLE STAGING.WEBLOGS_CLEAN (
    log_id       NUMBER,
    user_id      NUMBER,
    product_id   NUMBER,
    session_id   VARCHAR(100),
    action       VARCHAR(50),
    action_ts    TIMESTAMP_NTZ,
    etl_run_id   VARCHAR(36),
    etl_run_date DATE
);

CREATE OR REPLACE TABLE STAGING.USERS_CLEAN (
    user_id      NUMBER,
    user_name    VARCHAR(255),
    email        VARCHAR(255),
    signup_date  VARCHAR(50)
);

CREATE OR REPLACE TABLE STAGING.PRODUCTS_CLEAN (
    product_id    NUMBER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT
);
