-- ANALYTICS schema (Gold layer) — verbatim from spec Phase 4 "Gold Tables — Snowflake DDL".

-- Dimension: User
CREATE OR REPLACE TABLE ANALYTICS.DIM_USER (
    user_sk        NUMBER AUTOINCREMENT PRIMARY KEY,  -- surrogate key
    user_id        NUMBER NOT NULL,
    user_name      VARCHAR(255),
    email          VARCHAR(255),
    signup_date    DATE,
    dw_created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Product
CREATE OR REPLACE TABLE ANALYTICS.DIM_PRODUCT (
    product_sk     NUMBER AUTOINCREMENT PRIMARY KEY,
    product_id     NUMBER NOT NULL,
    product_name   VARCHAR(255),
    category       VARCHAR(100),
    price          FLOAT,
    dw_created_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Fact: User Activity
CREATE OR REPLACE TABLE ANALYTICS.FACT_USER_ACTIVITY (
    activity_id    NUMBER AUTOINCREMENT PRIMARY KEY,
    log_id         NUMBER,
    user_sk        NUMBER REFERENCES ANALYTICS.DIM_USER(user_sk),
    product_sk     NUMBER REFERENCES ANALYTICS.DIM_PRODUCT(product_sk),
    session_id     VARCHAR(100),
    action         VARCHAR(50),
    action_ts      TIMESTAMP_NTZ,
    etl_run_id     VARCHAR(36),
    etl_run_date   DATE
)
CLUSTER BY (etl_run_date, action);   -- Snowflake micro-partition clustering

-- Aggregate: Session Metrics
CREATE OR REPLACE TABLE ANALYTICS.AGG_SESSION_METRICS (
    session_id          VARCHAR(100) PRIMARY KEY,
    user_sk             NUMBER REFERENCES ANALYTICS.DIM_USER(user_sk),
    session_start       TIMESTAMP_NTZ,
    session_end         TIMESTAMP_NTZ,
    session_duration_s  FLOAT,
    total_actions       NUMBER,
    total_views         NUMBER,
    total_cart_adds     NUMBER,
    total_purchases     NUMBER,
    conversion_rate     FLOAT,
    is_abandoned_cart   BOOLEAN,
    is_high_activity    BOOLEAN,
    etl_run_date        DATE
)
CLUSTER BY (etl_run_date);
