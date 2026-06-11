import pandas as pd
import numpy as np
import uuid
import logging
from datetime import datetime, date

from etl.config import config
from etl.connections.s3_client import StorageClient
from etl.connections.snowflake_client import SnowflakeClient
from etl.helpers.validators import DataValidator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebLogPipeline:
    def __init__(self):
        self.etl_run_id = str(uuid.uuid4())
        self.etl_run_date = date.today().isoformat()
        self.etl_run_ts = datetime.utcnow().isoformat()
        
        self.storage = StorageClient()
        self.snowflake = SnowflakeClient()
        
        run_context = {
            'etl_run_id': self.etl_run_id,
            'etl_run_date': self.etl_run_date,
            'etl_run_timestamp': self.etl_run_ts
        }
        self.validator = DataValidator(run_context)

    # ── Bronze Phase ──────────────────────────────────────
    def extract(self):
        logger.info("Extracting data and writing to Bronze...")
        
        # Process Weblogs in Chunks
        try:
            for idx, chunk in enumerate(pd.read_csv(config.WEBLOG_FILE, chunksize=config.CHUNK_SIZE)):
                clean_chunk, anomaly_chunk = self.validator.validate_weblogs(chunk, config.WEBLOG_FILE)
                
                if not clean_chunk.empty:
                    self.storage.write_parquet(clean_chunk, f"bronze/weblogs/ingest_date={self.etl_run_date}/weblogs_chunk_{idx:03d}.parquet")
                
                if not anomaly_chunk.empty:
                    self.storage.write_parquet(anomaly_chunk, f"quarantine/source=weblogs/etl_run_date={self.etl_run_date}/etl_run_id={self.etl_run_id}/anomalies_chunk_{idx:03d}.parquet")
        except FileNotFoundError:
            logger.warning(f"File not found: {config.WEBLOG_FILE}")
            
        # Process Users
        try:
            users_df = pd.read_csv(config.USERS_FILE)
            clean_users, anomaly_users = self.validator.validate_users(users_df, config.USERS_FILE)
            if not clean_users.empty:
                self.storage.write_parquet(clean_users, f"bronze/users/ingest_date={self.etl_run_date}/users.parquet")
            if not anomaly_users.empty:
                self.storage.write_parquet(anomaly_users, f"quarantine/source=users/etl_run_date={self.etl_run_date}/etl_run_id={self.etl_run_id}/anomalies.parquet")
        except FileNotFoundError:
            logger.warning(f"File not found: {config.USERS_FILE}")

        # Process Products
        try:
            products_df = pd.read_csv(config.PRODUCTS_FILE)
            clean_products, anomaly_products = self.validator.validate_products(products_df, config.PRODUCTS_FILE)
            if not clean_products.empty:
                self.storage.write_parquet(clean_products, f"bronze/products/ingest_date={self.etl_run_date}/products.parquet")
            if not anomaly_products.empty:
                self.storage.write_parquet(anomaly_products, f"quarantine/source=products/etl_run_date={self.etl_run_date}/etl_run_id={self.etl_run_id}/anomalies.parquet")
        except FileNotFoundError:
             logger.warning(f"File not found: {config.PRODUCTS_FILE}")

    # ── Silver Phase ──────────────────────────────────────
    def transform(self):
        logger.info("Transforming data to Silver layer...")
        
        weblogs = self.storage.read_parquet(f"bronze/weblogs/ingest_date={self.etl_run_date}/")
        users = self.storage.read_parquet(f"bronze/users/ingest_date={self.etl_run_date}/")
        products = self.storage.read_parquet(f"bronze/products/ingest_date={self.etl_run_date}/")
        
        if weblogs.empty:
            logger.warning("No weblogs found to transform. Skipping Silver phase.")
            return

        # Sort out-of-order logs
        weblogs['timestamp'] = pd.to_datetime(weblogs['timestamp'])
        weblogs = weblogs.sort_values(by=['session_id', 'timestamp'])
        
        # Calculate session metrics (for aggregations)
        session_metrics = weblogs.groupby('session_id').agg(
            session_start=('timestamp', 'min'),
            session_end=('timestamp', 'max'),
            total_actions=('action', 'count'),
            total_views=('action', lambda x: (x == 'view').sum()),
            total_cart_adds=('action', lambda x: (x == 'add_to_cart').sum()),
            total_purchases=('action', lambda x: (x == 'purchase').sum())
        ).reset_index()

        # Compute duration using NumPy
        session_metrics['session_duration_s'] = np.where(
            pd.notna(session_metrics['session_end']),
            (session_metrics['session_end'] - session_metrics['session_start']).dt.total_seconds(),
            0
        )

        # Compute conversion rate
        session_metrics['conversion_rate'] = np.where(
            session_metrics['total_views'] > 0,
            session_metrics['total_purchases'] / session_metrics['total_views'],
            0
        )

        self.storage.write_parquet(weblogs, f"silver/weblogs_clean/etl_run_date={self.etl_run_date}/etl_run_id={self.etl_run_id}/weblogs_silver.parquet")
        
        if not users.empty:
            self.storage.write_parquet(users, f"silver/users_clean/etl_run_date={self.etl_run_date}/users_silver.parquet")
        if not products.empty:
            self.storage.write_parquet(products, f"silver/products_clean/etl_run_date={self.etl_run_date}/products_silver.parquet")
        
        logger.info("Saved transformed data to Silver layer.")

    # ── Gold Phase ────────────────────────────────────────
    def load_to_snowflake(self, layer: str):
        logger.info(f"Loading {layer} layer into Snowflake...")
        
        # In a real environment, we would use COPY INTO commands pointing to the S3 bucket.
        # We assume the external stages are already setup.
        if layer == 'bronze':
            self.snowflake.execute("COPY INTO RAW.BRONZE_WEBLOGS FROM @bronze_stage/weblogs/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
            self.snowflake.execute("COPY INTO RAW.BRONZE_USERS FROM @bronze_stage/users/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
            self.snowflake.execute("COPY INTO RAW.BRONZE_PRODUCTS FROM @bronze_stage/products/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
        elif layer == 'silver':
            self.snowflake.execute("COPY INTO STAGING.WEBLOGS_CLEAN FROM @silver_stage/weblogs_clean/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
            self.snowflake.execute("COPY INTO STAGING.USERS_CLEAN FROM @silver_stage/users_clean/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
            self.snowflake.execute("COPY INTO STAGING.PRODUCTS_CLEAN FROM @silver_stage/products_clean/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")

    def build_gold(self):
        logger.info("Building Gold tables in Snowflake (Running SQL DML)...")
        # Run specific sql scripts for DML
        dml_scripts = [
            "sql/dml/merge_dim_user.sql",
            "sql/dml/merge_dim_product.sql",
            "sql/dml/load_fact_user_activity.sql",
            "sql/dml/build_agg_session_metrics.sql"
        ]
        
        for script in dml_scripts:
            try:
                logger.info(f"Running script: {script}")
                self.snowflake.run_script(script)
            except Exception as e:
                logger.error(f"Failed to execute {script}: {e}")

    # ── Orchestration ─────────────────────────────────────
    def run(self):
        logger.info(f"--- Starting ETL Pipeline Run: {self.etl_run_id} ---")
        try:
            self.extract()
            self.transform()
            self.load_to_snowflake('bronze')
            self.load_to_snowflake('silver')
            self.build_gold()
            logger.info("--- Pipeline Completed Successfully ---")
        except Exception as e:
            logger.error(f"--- Pipeline Failed: {e} ---")

if __name__ == "__main__":
    pipeline = WebLogPipeline()
    pipeline.run()
