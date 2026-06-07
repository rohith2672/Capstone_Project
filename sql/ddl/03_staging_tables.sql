-- STAGING schema (Silver mirror) — spec names these tables (lines ~298-300) but does
-- not give their column DDL, so this is INFERRED to mirror exactly what write_silver()
-- persists (see processor.transform()/enrich()):
--   WEBLOGS_CLEAN   = parsed/categorized/sorted weblogs + embedded run metadata
--                     (etl_run_id/etl_run_date — see processor.transform docstring for
--                     why these are columns, not just S3 partition keys) + the
--                     user/product enrichment columns brought in by enrich()'s joins.
--   USERS_CLEAN / PRODUCTS_CLEAN = the validated reference snapshots (re-read from
--                     Bronze; same shape as RAW.BRONZE_USERS/PRODUCTS minus _loaded_at).
-- Loaded via `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`.

CREATE OR REPLACE TABLE STAGING.WEBLOGS_CLEAN (
    log_id        NUMBER,
    user_id       NUMBER,
    product_id    NUMBER,
    session_id    VARCHAR(100),
    action        VARCHAR(50),
    "timestamp"   VARCHAR(50),
    action_ts     TIMESTAMP_NTZ,
    etl_run_id    VARCHAR(36),
    etl_run_date  DATE,
    user_name     VARCHAR(255),
    email         VARCHAR(255),
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT,
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.USERS_CLEAN (
    user_id       NUMBER,
    user_name     VARCHAR(255),
    email         VARCHAR(255),
    -- DATE (not VARCHAR, unlike RAW.BRONZE_USERS): Bronze validation already
    -- quarantines unparseable signup_date strings (see processor._validate_users),
    -- so every value reaching here is a valid ISO date — COPY INTO converts it
    -- automatically, and merge_dim_user.sql inserts source.signup_date straight
    -- into DIM_USER.signup_date (DATE) with no explicit cast (spec verbatim MERGE).
    signup_date   DATE,
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE STAGING.PRODUCTS_CLEAN (
    product_id    NUMBER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT,
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
