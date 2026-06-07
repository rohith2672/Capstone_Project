-- Inserts one ETL_AUDIT_LOG row per Medallion layer per run (RunMetrics.to_audit_rows
-- builds exactly that list of dicts — see metrics.py for why bronze/silver/gold each
-- get their own row). Run via SnowflakeLoader.executemany(sql, rows): %(name)s
-- placeholders bind against each dict's keys (pyformat paramstyle).
INSERT INTO ANALYTICS.ETL_AUDIT_LOG (
    etl_run_id, etl_run_date, etl_run_timestamp, source_file, layer,
    rows_extracted, rows_quarantined, rows_loaded, quarantine_s3_path,
    status, error_message
) VALUES (
    %(etl_run_id)s, %(etl_run_date)s, %(etl_run_timestamp)s, %(source_file)s, %(layer)s,
    %(rows_extracted)s, %(rows_quarantined)s, %(rows_loaded)s, %(quarantine_s3_path)s,
    %(status)s, %(error_message)s
)
