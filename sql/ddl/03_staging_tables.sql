-- STAGING schema (Silver mirror) — tables mirror the columns written to S3 Silver
-- by WebLogProcessor.write_silver() for each dataset.

CREATE SCHEMA IF NOT EXISTS STAGING;
USE SCHEMA STAGING;

-- Silver: Users (cleaned)
-- is_duplicate_email / is_invalid_email are flags computed during Bronze validation
-- (duplicate/invalid emails are no longer rejected — see _validate_users()).
CREATE OR REPLACE TABLE STAGING.USERS_CLEAN (
    user_id            NUMBER,
    user_name          VARCHAR(255),
    email              VARCHAR(255),
    signup_date        DATE,
    is_duplicate_email BOOLEAN,
    is_invalid_email   BOOLEAN
);

-- Silver: Products (cleaned)
CREATE OR REPLACE TABLE STAGING.PRODUCTS_CLEAN (
    product_id    NUMBER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT
);

-- Silver: Weblogs (cleaned, enriched)
CREATE OR REPLACE TABLE STAGING.WEBLOGS_CLEAN (
    log_id        NUMBER,
    user_id       NUMBER,
    product_id    NUMBER,
    session_id    VARCHAR(100),
    action        VARCHAR(50),
    timestamp     VARCHAR(50),
    action_ts     TIMESTAMP_NTZ,
    etl_run_id    VARCHAR(36),
    etl_run_date  DATE,
    user_name     VARCHAR(255),
    email         VARCHAR(255),
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT
);
