-- RAW schema (Bronze mirror) — landing tables for COPY INTO RAW.BRONZE_* in
-- pipeline.py load_to_snowflake('bronze'), loaded from S3 Bronze Parquet via
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE. Columns mirror the source CSVs as-is
-- (untransformed, append-only).

CREATE SCHEMA IF NOT EXISTS RAW;

CREATE OR REPLACE TABLE RAW.BRONZE_WEBLOGS (
    log_id      NUMBER,
    user_id     NUMBER,
    product_id  NUMBER,
    session_id  VARCHAR(100),
    action      VARCHAR(50),
    timestamp   VARCHAR(50)
);

CREATE OR REPLACE TABLE RAW.BRONZE_USERS (
    user_id      NUMBER,
    user_name    VARCHAR(255),
    email        VARCHAR(255),
    signup_date  VARCHAR(50)
);

CREATE OR REPLACE TABLE RAW.BRONZE_PRODUCTS (
    product_id    NUMBER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT
);
