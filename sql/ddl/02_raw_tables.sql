-- RAW schema (Bronze mirror) — spec names these tables (lines ~294-296) but does not
-- give their column DDL, so this is INFERRED to mirror exactly what write_bronze()
-- persists: the validated source-CSV columns, unchanged (no parsing/typing applied —
-- that happens in Silver, e.g. timestamp -> action_ts). Loaded via
-- `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`, so column NAMES must match
-- the Parquet files' columns; types are widened where useful (e.g. raw timestamp/
-- signup_date kept as strings here — see README Assumptions for why parsing is
-- deferred to Silver rather than done twice, in Python and SQL).

CREATE OR REPLACE TABLE RAW.BRONZE_WEBLOGS (
    log_id        NUMBER,
    user_id       NUMBER,
    product_id    NUMBER,
    session_id    VARCHAR(100),
    action        VARCHAR(50),
    "timestamp"   VARCHAR(50),
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.BRONZE_USERS (
    user_id       NUMBER,
    user_name     VARCHAR(255),
    email         VARCHAR(255),
    signup_date   VARCHAR(50),
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW.BRONZE_PRODUCTS (
    product_id    NUMBER,
    product_name  VARCHAR(255),
    category      VARCHAR(100),
    price         FLOAT,
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
