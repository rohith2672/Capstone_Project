-- ETL_AUDIT_LOG — verbatim from spec Phase 6 "ETL Audit Log — Snowflake Table"
-- (lines ~466-480). One row per Medallion layer per run is inserted via
-- sql/dml/insert_audit_log.sql + SnowflakeLoader.executemany — see
-- RunMetrics.to_audit_rows for why (one ETL_AUDIT_LOG row per (run, layer)).

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
