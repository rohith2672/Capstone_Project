-- RAW schema (Bronze mirror) — tables mirror the columns written to S3 Bronze
-- by WebLogProcessor.write_bronze() for each source.

CREATE SCHEMA IF NOT EXISTS RAW;
USE SCHEMA RAW;

-- Bronze: Users
-- is_duplicate_email / is_invalid_email are flags computed during Bronze validation
-- (duplicate/invalid emails are no longer rejected — see _validate_users()).
CREATE OR REPLACE TABLE RAW.BRONZE_USERS (
    user_id            NUMBER,
    user_name          VARCHAR(255),
    email              VARCHAR(255),
    signup_date        DATE,
    is_duplicate_email BOOLEAN,
    is_invalid_email   BOOLEAN
);

-- Bronze: Products
CREATE OR REPLACE TABLE RAW.BRONZE_PRODUCTS (
    product_id    NUMBER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT
);

-- Bronze: Weblogs
CREATE OR REPLACE TABLE RAW.BRONZE_WEBLOGS (
    log_id      NUMBER,
    user_id     NUMBER,
    product_id  NUMBER,
    session_id  VARCHAR(100),
    action      VARCHAR(50),
    timestamp   VARCHAR(50)
);
