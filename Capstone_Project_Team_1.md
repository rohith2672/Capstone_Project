# Capstone Project — Team 1: E-Commerce Web Log ETL Pipeline

## Scenario

An e-commerce platform wants to analyze **user behavior** from web logs. The goal is to design a **Python-based ETL pipeline** following the **Medallion Architecture (Bronze → Silver → Gold)** that cleans and parses log data, enriches it with user and product info, stages data through S3, and loads analytical tables into **Snowflake** for BI consumption.

---

## Architecture Overview

```
[Raw CSVs]
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Amazon S3                                │
│                                                                 │
│  bronze/                   silver/              gold/           │
│  └── weblogs/              └── weblogs/         └── weblogs/    │
│  └── users/                └── users/           └── users/      │
│  └── products/             └── products/        └── products/   │
│                                                                 │
│  quarantine/                                                    │
│  └── anomalies/                                                 │
│      └── etl_run_date=YYYY-MM-DD/                               │
│          └── etl_run_id=<uuid>/                                 │
│              └── weblogs_anomalies.parquet                      │
└─────────────────────────────────────────────────────────────────┘
    │                                   │
    ▼                                   ▼
[Snowflake — RAW schema]        [Snowflake — ANALYTICS schema]
  BRONZE_WEBLOGS                  dim_user
  BRONZE_USERS                    dim_product
  BRONZE_PRODUCTS                 fact_user_activity
                                  agg_session_metrics
                                  etl_audit_log
```

---

## Medallion Architecture

The pipeline follows the standard three-layer Medallion pattern:

| Layer | S3 Prefix | Snowflake Schema | Description |
|---|---|---|---|
| **Bronze** | `s3://bucket/bronze/` | `RAW` | Raw ingested data, no transformations, append-only |
| **Silver** | `s3://bucket/silver/` | `STAGING` | Cleaned, validated, deduplicated, joined data |
| **Gold** | `s3://bucket/gold/` | `ANALYTICS` | Aggregated, business-ready fact & dimension tables |
| **Quarantine** | `s3://bucket/quarantine/` | — | Anomalous / rejected rows, partitioned by run for auditability |

---

## ETL Objectives

- Parse large log files in chunks
- Land raw data in S3 Bronze with zero transformation
- Clean and normalize in Silver; quarantine anomalies immediately
- Build fact and dimension tables in Gold / Snowflake ANALYTICS schema
- Enable behavioral and conversion analysis via Snowflake SQL

---

## Phase 1 — Extract → Bronze Layer (S3 + Snowflake RAW)

### Core Tasks

- Load CSV files in **chunks** using Pandas (`chunksize=10000`) to handle large files
- Validate schema on each chunk:
  - Missing columns
  - Invalid data types
- Handle missing or null user IDs
- Detect duplicates in log entries

### Anomaly Quarantine — S3 (Enterprise Pattern)

All rows that fail validation are **immediately separated** from clean rows and written to a dedicated S3 quarantine prefix. Do **not** mix bad rows into the Bronze layer.

**Recommended S3 path structure:**

```
s3://<bucket>/quarantine/
└── source=weblogs/
    └── etl_run_date=2024-06-07/
        └── etl_run_id=3f2a1b9c-<uuid>/
            └── anomalies.parquet
```

> **Why `etl_run_date` + `etl_run_id` over a single timestamp?**
>
> - `etl_run_date` (date only) lets you **list all failures for a calendar day** with a single S3 prefix scan — useful for daily ops dashboards and Athena/Glue partition discovery.
> - `etl_run_id` (UUID) is immutable and unique per run, so **re-runs on the same day don't overwrite each other**.
> - A single `etl_run_timestamp` like `2024-06-07T14:32:00` works but makes prefix scanning awkward (you can't query "all runs on June 7" without a wildcard that also hits June 17).
> - This two-level pattern is standard in enterprise lakehouse teams at AWS, Databricks, and Snowflake.

**Quarantine record schema** — each rejected row must carry:

| Column | Description |
|---|---|
| `etl_run_id` | UUID of the pipeline run |
| `etl_run_date` | Date of the run (`YYYY-MM-DD`) |
| `etl_run_timestamp` | Full ISO timestamp for exact ordering |
| `source_file` | Which input file the row came from |
| `row_index` | Original row position in the source file |
| `rejection_reason` | Human-readable reason (e.g., `"null session_id"`, `"invalid timestamp"`) |
| `raw_row` | JSON-serialized original row for forensics |

**Python pattern:**

```python
import uuid
from datetime import datetime, date

ETL_RUN_ID = str(uuid.uuid4())
ETL_RUN_DATE = date.today().isoformat()          # "2024-06-07"
ETL_RUN_TS   = datetime.utcnow().isoformat()     # "2024-06-07T14:32:00.123456"

QUARANTINE_PREFIX = (
    f"s3://<bucket>/quarantine/"
    f"source=weblogs/"
    f"etl_run_date={ETL_RUN_DATE}/"
    f"etl_run_id={ETL_RUN_ID}/"
    f"anomalies.parquet"
)
```

### Additional Extraction Tasks

- Detect **orphan product IDs** (log entries for products not in the products table)
- Detect **invalid session IDs** (null or malformed)
- Write all anomalous rows to S3 quarantine (see above)
- Write **clean rows** to S3 Bronze as Parquet, partitioned by `ingest_date`

### Bronze S3 Layout

```
s3://<bucket>/bronze/
├── weblogs/ingest_date=2024-06-07/weblogs_chunk_001.parquet
├── users/ingest_date=2024-06-07/users.parquet
└── products/ingest_date=2024-06-07/products.parquet
```

### Snowflake — RAW Schema (Bronze Mirror)

Load Bronze Parquet files into Snowflake using `COPY INTO` via a named S3 stage:

```sql
-- Create external stage pointing to Bronze S3 prefix
CREATE OR REPLACE STAGE bronze_stage
  URL = 's3://<bucket>/bronze/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
  FILE_FORMAT = (TYPE = PARQUET);

-- Load raw weblogs
COPY INTO RAW.BRONZE_WEBLOGS
FROM @bronze_stage/weblogs/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = CONTINUE;  -- log failures, don't halt the run
```

---

## Phase 2 — Transform → Silver Layer (S3 + Snowflake STAGING)

### Core Transformations

- Parse timestamps into `datetime` objects; reject unparseable rows to quarantine
- Categorize user actions: `view`, `add_to_cart`, `purchase`
- Compute **session duration** using NumPy: `session_end - session_start` per session ID per user
- Aggregate metrics per session:
  - Total actions
  - Total products viewed / purchased
- Merge logs with `users` and `products` for enrichment

### Advanced Transform Tasks

- Compute **conversion rate** per session: `purchases / views`
- Identify **abandoned carts**: sessions with `add_to_cart` but no `purchase`
- Flag **high-activity sessions**: sessions with > 50 actions
- Handle **out-of-order logs**: sort by timestamp per session
- Vectorize session duration computation using NumPy (no loops)

### Silver S3 Layout

```
s3://<bucket>/silver/
├── weblogs_clean/etl_run_date=2024-06-07/etl_run_id=<uuid>/weblogs_silver.parquet
├── users_clean/etl_run_date=2024-06-07/users_silver.parquet
└── products_clean/etl_run_date=2024-06-07/products_silver.parquet
```

### Snowflake — STAGING Schema (Silver Mirror)

```sql
COPY INTO STAGING.WEBLOGS_CLEAN
FROM @silver_stage/weblogs_clean/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

---

## Phase 3 — Python Engineering Requirements

### Functional Programming

Write reusable functions for:
- Schema validation
- Missing value handling
- Deduplication
- Metric calculations
- Quarantine row writing

### OOP Design

Implement the following class (updated for Medallion + Snowflake):

```python
class WebLogProcessor:
    def __init__(self, weblog_file, users_file, products_file,
                 s3_bucket: str, snowflake_conn_params: dict):
        self.etl_run_id   = str(uuid.uuid4())
        self.etl_run_date = date.today().isoformat()
        self.etl_run_ts   = datetime.utcnow().isoformat()
        # ... store other params

    # ── Bronze Phase ──────────────────────────────────────
    def extract(self, chunk_size=10000):
        """Read source CSVs in chunks."""
        pass

    def validate(self, chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return (clean_rows, anomaly_rows)."""
        pass

    def write_bronze(self, clean_chunk: pd.DataFrame):
        """Write clean rows to S3 bronze/ as Parquet."""
        pass

    def write_quarantine(self, anomaly_chunk: pd.DataFrame, source: str):
        """Write rejected rows to S3 quarantine/ with run metadata."""
        pass

    # ── Silver Phase ──────────────────────────────────────
    def transform(self):
        """Apply business logic transformations on Bronze data."""
        pass

    def enrich(self):
        """Join weblogs with users and products."""
        pass

    def write_silver(self, df: pd.DataFrame):
        """Write enriched data to S3 silver/ as Parquet."""
        pass

    # ── Gold Phase ────────────────────────────────────────
    def load_to_snowflake(self, layer: str):
        """COPY INTO Snowflake from S3 stage (bronze, silver, or gold)."""
        pass

    def build_gold(self):
        """Run Gold-layer SQL in Snowflake (CREATE TABLE AS SELECT)."""
        pass

    # ── Orchestration ─────────────────────────────────────
    def run(self):
        """Orchestrate full Bronze → Silver → Gold pipeline."""
        pass
```

### Additional Python Requirements

- Create **helper functions** for:
  - Parsing timestamps
  - Categorizing actions
  - Computing session metrics
  - Generating S3 quarantine paths
- Implement **structured logging** (JSON format) for ETL steps and errors
- Handle **corrupt rows** gracefully — quarantine and continue, never crash
- Track **ETL metrics**: rows extracted, rows quarantined, rows loaded per layer
- Implement **unit tests** for key transformations and quarantine logic

---

## Phase 4 — Load → Gold Layer (Snowflake ANALYTICS)

### Snowflake Database & Schema Layout

```
Database: ECOMMERCE_DW
├── Schema: RAW          ← Bronze mirror
│   ├── BRONZE_WEBLOGS
│   ├── BRONZE_USERS
│   └── BRONZE_PRODUCTS
├── Schema: STAGING      ← Silver mirror
│   ├── WEBLOGS_CLEAN
│   ├── USERS_CLEAN
│   └── PRODUCTS_CLEAN
└── Schema: ANALYTICS    ← Gold (business-ready)
    ├── DIM_USER
    ├── DIM_PRODUCT
    ├── FACT_USER_ACTIVITY
    ├── AGG_SESSION_METRICS
    └── ETL_AUDIT_LOG
```

### Gold Tables — Snowflake DDL

```sql
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
```

### Upsert Logic (MERGE INTO)

Use Snowflake's `MERGE` for idempotent dimension loads:

```sql
MERGE INTO ANALYTICS.DIM_USER AS target
USING STAGING.USERS_CLEAN AS source
ON target.user_id = source.user_id
WHEN MATCHED THEN UPDATE SET
    user_name     = source.user_name,
    email         = source.email,
    dw_updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (user_id, user_name, email, signup_date)
VALUES
    (source.user_id, source.user_name, source.email, source.signup_date);
```

---

## Phase 5 — SQL Analytics in Snowflake (Business Intelligence)

All queries run against `ANALYTICS` schema in Snowflake.

### Core Queries to Implement

```sql
-- 1. Most viewed products
SELECT p.product_name, COUNT(*) AS view_count
FROM ANALYTICS.FACT_USER_ACTIVITY f
JOIN ANALYTICS.DIM_PRODUCT p ON f.product_sk = p.product_sk
WHERE f.action = 'view'
GROUP BY 1 ORDER BY 2 DESC LIMIT 20;

-- 2. Conversion rate (sessions with purchase / sessions with any activity)
SELECT
    COUNT(DISTINCT CASE WHEN total_purchases > 0 THEN session_id END)::FLOAT
    / NULLIF(COUNT(DISTINCT session_id), 0) AS overall_conversion_rate
FROM ANALYTICS.AGG_SESSION_METRICS;

-- 3. Average session duration
SELECT AVG(session_duration_s) / 60 AS avg_duration_minutes
FROM ANALYTICS.AGG_SESSION_METRICS
WHERE session_duration_s >= 0;

-- 4. Top users by total purchases
SELECT u.user_name, SUM(s.total_purchases) AS total_purchases
FROM ANALYTICS.AGG_SESSION_METRICS s
JOIN ANALYTICS.DIM_USER u ON s.user_sk = u.user_sk
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- 5. Abandoned cart rate per day
SELECT etl_run_date,
    SUM(CASE WHEN is_abandoned_cart THEN 1 ELSE 0 END)::FLOAT
    / NULLIF(COUNT(*), 0) AS abandoned_cart_rate
FROM ANALYTICS.AGG_SESSION_METRICS
GROUP BY 1 ORDER BY 1;

-- 6. Product conversion rate (purchases / views per product)
SELECT p.product_name,
    SUM(CASE WHEN f.action='purchase' THEN 1 ELSE 0 END)::FLOAT
    / NULLIF(SUM(CASE WHEN f.action='view' THEN 1 ELSE 0 END), 0) AS product_cvr
FROM ANALYTICS.FACT_USER_ACTIVITY f
JOIN ANALYTICS.DIM_PRODUCT p ON f.product_sk = p.product_sk
GROUP BY 1 ORDER BY 2 DESC NULLS LAST;

-- 7. Average actions per session
SELECT AVG(total_actions) AS avg_actions_per_session
FROM ANALYTICS.AGG_SESSION_METRICS;

-- 8. Peak activity hours
SELECT HOUR(action_ts) AS hour_of_day, COUNT(*) AS action_count
FROM ANALYTICS.FACT_USER_ACTIVITY
GROUP BY 1 ORDER BY 1;

-- 9. Unusually long sessions (> 2 std deviations above mean)
SELECT session_id, session_duration_s
FROM ANALYTICS.AGG_SESSION_METRICS
WHERE session_duration_s > (
    SELECT AVG(session_duration_s) + 2 * STDDEV(session_duration_s)
    FROM ANALYTICS.AGG_SESSION_METRICS
);

-- 10. Cohort analysis: users by signup month vs purchases
SELECT
    DATE_TRUNC('month', u.signup_date) AS signup_cohort,
    SUM(s.total_purchases) AS total_purchases,
    COUNT(DISTINCT s.user_sk) AS active_users
FROM ANALYTICS.AGG_SESSION_METRICS s
JOIN ANALYTICS.DIM_USER u ON s.user_sk = u.user_sk
GROUP BY 1 ORDER BY 1;
```

---

## Phase 6 — Data Quality & Auditing

### ETL Audit Log — Snowflake Table

```sql
CREATE OR REPLACE TABLE ANALYTICS.ETL_AUDIT_LOG (
    audit_id            NUMBER AUTOINCREMENT PRIMARY KEY,
    etl_run_id          VARCHAR(36) NOT NULL,
    etl_run_date        DATE,
    etl_run_timestamp   TIMESTAMP_NTZ,
    source_file         VARCHAR(500),
    layer               VARCHAR(20),   -- 'bronze', 'silver', 'gold'
    rows_extracted      NUMBER,
    rows_quarantined    NUMBER,
    rows_loaded         NUMBER,
    quarantine_s3_path  VARCHAR(1000),
    status              VARCHAR(20),   -- 'SUCCESS', 'PARTIAL', 'FAILED'
    error_message       VARCHAR(5000),
    created_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### Python Audit

Track and insert the following per ETL run:
- Rows extracted, transformed, loaded (per layer)
- Rows quarantined + quarantine S3 path
- Session anomalies count
- Run status (`SUCCESS` / `PARTIAL` / `FAILED`)

### SQL Validation Queries (Post-Load Checks)

```sql
-- No orphan session IDs (fact rows with no matching dim)
SELECT COUNT(*) AS orphan_users
FROM ANALYTICS.FACT_USER_ACTIVITY
WHERE user_sk NOT IN (SELECT user_sk FROM ANALYTICS.DIM_USER);

-- No duplicate log entries in fact table
SELECT log_id, COUNT(*) AS cnt
FROM ANALYTICS.FACT_USER_ACTIVITY
GROUP BY 1 HAVING cnt > 1;

-- All session durations are non-negative
SELECT COUNT(*) AS invalid_duration_count
FROM ANALYTICS.AGG_SESSION_METRICS
WHERE session_duration_s < 0;
```

---

## Phase 7 — Performance & Optimization (Snowflake-Specific)

### Python

- Use **vectorized operations** (NumPy/Pandas) — avoid explicit Python loops
- Write Parquet (not CSV) to S3 — columnar format, ~3–5x smaller, faster `COPY INTO`
- Use **multi-threaded S3 uploads** via `boto3` `TransferConfig`

### Snowflake

- Use **`CLUSTER BY`** on fact tables instead of traditional indexes (Snowflake doesn't have indexes)
- Use **`RESULT_CACHE`** — identical queries served from cache automatically; no action needed, but design queries to be deterministic
- Use **`EXPLAIN`** to analyze query plans:
  ```sql
  EXPLAIN SELECT * FROM ANALYTICS.FACT_USER_ACTIVITY WHERE etl_run_date = '2024-06-07';
  ```
- Use **`WAREHOUSE` sizing** — scale up to MEDIUM/LARGE for Gold aggregation runs, scale back to X-SMALL for BI queries
- Consider **Materialized Views** for the most expensive BI queries:
  ```sql
  CREATE MATERIALIZED VIEW ANALYTICS.MV_PRODUCT_DAILY_STATS AS
  SELECT product_sk, etl_run_date,
         SUM(CASE WHEN action='view' THEN 1 ELSE 0 END) AS views,
         SUM(CASE WHEN action='purchase' THEN 1 ELSE 0 END) AS purchases
  FROM ANALYTICS.FACT_USER_ACTIVITY
  GROUP BY 1, 2;
  ```

---

## S3 Bucket Layout (Complete)

```
s3://<bucket>/
├── bronze/
│   ├── weblogs/ingest_date=YYYY-MM-DD/
│   ├── users/ingest_date=YYYY-MM-DD/
│   └── products/ingest_date=YYYY-MM-DD/
│
├── silver/
│   ├── weblogs_clean/etl_run_date=YYYY-MM-DD/etl_run_id=<uuid>/
│   ├── users_clean/etl_run_date=YYYY-MM-DD/
│   └── products_clean/etl_run_date=YYYY-MM-DD/
│
├── gold/
│   └── (optional export snapshots for external consumers)
│
└── quarantine/
    ├── source=weblogs/
    │   └── etl_run_date=YYYY-MM-DD/
    │       └── etl_run_id=<uuid>/
    │           └── anomalies.parquet
    └── source=users/
        └── etl_run_date=YYYY-MM-DD/
            └── etl_run_id=<uuid>/
                └── anomalies.parquet
```

---

## Deliverables

Students must submit:

- [ ] `etl/` — Python ETL code (`WebLogProcessor` class + helpers + unit tests)
- [ ] `sql/ddl/` — Snowflake DDL scripts (all schemas, tables, stages, clustering)
- [ ] `sql/dml/` — MERGE/upsert scripts + audit log inserts
- [ ] `sql/analytics/` — All 10 BI queries (Phase 5)
- [ ] `sql/validation/` — Post-load data quality checks (Phase 6)
- [ ] `data_quality_report.md` — Generated by the ETL pipeline per run
- [ ] `diagrams/erd.png` — ERD for Gold layer
- [ ] `diagrams/architecture.png` — Medallion + S3 + Snowflake flow diagram
- [ ] `README.md` — Architecture decisions, Snowflake setup, assumptions

---

## Sample Data Generator

Run this script **once locally** to generate the three CSV input files (`users.csv`, `products.csv`, `weblogs.csv`).

> **Dependencies:** `pip install pandas numpy faker`

```python
import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
np.random.seed(42)

# ─── Users ───────────────────────────────────────────────
users = []
for i in range(1, 1101):
    users.append({
        "user_id": i if random.random() > 0.03 else random.randint(1, 50),  # intentional duplicates
        "user_name": fake.name() if random.random() > 0.05 else None,
        "email": fake.email() if random.random() > 0.1 else "invalid_email",
        "signup_date": fake.date_between(start_date="-5y", end_date="today")
                       if random.random() > 0.05 else "invalid_date"
    })
users_df = pd.DataFrame(users)
users_df.to_csv("users.csv", index=False)

# ─── Products ────────────────────────────────────────────
categories = ["Electronics", "Clothing", "Home", "Books", "Sports"]
products = []
for i in range(1, 1101):
    products.append({
        "product_id": i if random.random() > 0.02 else random.randint(1, 100),
        "product_name": fake.word().title(),
        "category": random.choice(categories),
        "price": round(random.uniform(5, 500), 2) if random.random() > 0.1 else None
    })
products_df = pd.DataFrame(products)
products_df.to_csv("products.csv", index=False)

# ─── Web Logs ────────────────────────────────────────────
actions = ["view", "add_to_cart", "purchase"]
weblogs = []
for i in range(1, 15001):
    user_id = random.randint(1, 1200)      # range exceeds users table → orphans
    product_id = random.randint(1, 1200)   # range exceeds products table → orphans
    session_id = f"sess_{random.randint(1, 5000)}" if random.random() > 0.05 else None
    timestamp = datetime.now() - timedelta(
        days=random.randint(0, 365),
        seconds=random.randint(0, 86400)
    )
    if random.random() < 0.05:
        timestamp = "invalid_timestamp"    # intentional bad timestamps

    weblogs.append({
        "log_id": i if random.random() > 0.03 else random.randint(1, 200),
        "user_id": user_id if random.random() > 0.05 else None,
        "product_id": product_id,
        "session_id": session_id,
        "action": random.choice(actions),
        "timestamp": timestamp
    })
weblogs_df = pd.DataFrame(weblogs)
weblogs_df.to_csv("weblogs.csv", index=False)

print("Web log CSV files generated successfully!")
```

### Generated File Summary

| File | Rows | Intentional Issues |
|---|---|---|
| `users.csv` | ~1,100 | Duplicate user IDs, null names, invalid emails, invalid dates |
| `products.csv` | ~1,100 | Duplicate product IDs, null prices |
| `weblogs.csv` | ~15,000 | Duplicate log IDs, null user IDs, null session IDs, invalid timestamps, orphan user/product IDs |

---

## Star Schema — Gold Layer (Snowflake ANALYTICS)

```
              DIM_USER
                 │ user_sk
                 ▼
DIM_PRODUCT ──► FACT_USER_ACTIVITY ◄── session_id
(product_sk)          │
                       ▼
               AGG_SESSION_METRICS
                       │
                       ▼
                ETL_AUDIT_LOG
```

**`FACT_USER_ACTIVITY`** is the central fact table joining all dimensions, clustered by `(etl_run_date, action)`. **`AGG_SESSION_METRICS`** is a pre-aggregated Gold table built via Snowflake SQL `CREATE TABLE AS SELECT`, clustered by `etl_run_date`. **`ETL_AUDIT_LOG`** records every pipeline run for full lineage and observability.
