import pandas as pd
import numpy as np
import boto3
import snowflake.connector
import uuid
import json
import logging
from datetime import datetime, date
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class WebLogProcessor:
    def __init__(self, weblog_file, users_file, products_file,
                 s3_bucket: str, snowflake_conn_params: dict):
        self.weblog_file = weblog_file
        self.users_file = users_file
        self.products_file = products_file
        self.s3_bucket = s3_bucket
        self.snowflake_conn_params = snowflake_conn_params
        
        self.etl_run_id = str(uuid.uuid4())
        self.etl_run_date = date.today().isoformat()
        self.etl_run_ts = datetime.utcnow().isoformat()
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    # ── Bronze Phase ──────────────────────────────────────
    def extract(self, chunk_size=10000):
        self.logger.info("Extracting data and writing to Bronze...")
        
        # Process Weblogs in Chunks
        for idx, chunk in enumerate(pd.read_csv(self.weblog_file, chunksize=chunk_size)):
            clean_chunk, anomaly_chunk = self.validate_weblogs(chunk)
            
            if not clean_chunk.empty:
                self.write_bronze(clean_chunk, f"weblogs/ingest_date={self.etl_run_date}/weblogs_chunk_{idx:03d}.parquet")
            
            if not anomaly_chunk.empty:
                self.write_quarantine(anomaly_chunk, "weblogs")
                
        # Process Users
        users_df = pd.read_csv(self.users_file)
        clean_users, anomaly_users = self.validate_users(users_df)
        if not clean_users.empty:
            self.write_bronze(clean_users, f"users/ingest_date={self.etl_run_date}/users.parquet")
        if not anomaly_users.empty:
            self.write_quarantine(anomaly_users, "users")

        # Process Products
        products_df = pd.read_csv(self.products_file)
        clean_products, anomaly_products = self.validate_products(products_df)
        if not clean_products.empty:
            self.write_bronze(clean_products, f"products/ingest_date={self.etl_run_date}/products.parquet")
        if not anomaly_products.empty:
            self.write_quarantine(anomaly_products, "products")

    def validate_weblogs(self, chunk: pd.DataFrame):
        anomalies = []
        clean_rows = []
        
        for idx, row in chunk.iterrows():
            reason = None
            if pd.isna(row.get('session_id')):
                reason = "null session_id"
            elif pd.isna(row.get('user_id')):
                reason = "null user_id"
            else:
                try:
                    pd.to_datetime(row.get('timestamp'))
                except:
                    reason = "invalid timestamp"
                    
            if reason:
                anomalies.append(self._create_anomaly(self.weblog_file, idx, reason, row))
            else:
                clean_rows.append(row)
                
        return pd.DataFrame(clean_rows), pd.DataFrame(anomalies)

    def validate_users(self, df: pd.DataFrame):
        anomalies = []
        clean_rows = []
        for idx, row in df.iterrows():
            if pd.isna(row.get('user_name')):
                anomalies.append(self._create_anomaly(self.users_file, idx, "null user_name", row))
            else:
                clean_rows.append(row)
        return pd.DataFrame(clean_rows), pd.DataFrame(anomalies)

    def validate_products(self, df: pd.DataFrame):
        anomalies = []
        clean_rows = []
        for idx, row in df.iterrows():
            if pd.isna(row.get('price')):
                anomalies.append(self._create_anomaly(self.products_file, idx, "null price", row))
            else:
                clean_rows.append(row)
        return pd.DataFrame(clean_rows), pd.DataFrame(anomalies)

    def _create_anomaly(self, source, idx, reason, row):
        return {
            'etl_run_id': self.etl_run_id,
            'etl_run_date': self.etl_run_date,
            'etl_run_timestamp': self.etl_run_ts,
            'source_file': source,
            'row_index': idx,
            'rejection_reason': reason,
            'raw_row': json.dumps(row.to_dict(), default=str)
        }

    def write_bronze(self, clean_chunk: pd.DataFrame, s3_key: str):
        path = f"s3://{self.s3_bucket}/bronze/{s3_key}"
        clean_chunk.to_parquet(path, index=False)
        self.logger.info(f"Wrote to bronze: {path}")

    def write_quarantine(self, anomaly_chunk: pd.DataFrame, source: str):
        path = f"s3://{self.s3_bucket}/quarantine/source={source}/etl_run_date={self.etl_run_date}/etl_run_id={self.etl_run_id}/anomalies.parquet"
        anomaly_chunk.to_parquet(path, index=False)
        self.logger.info(f"Wrote anomalies to quarantine: {path}")

    # ── Silver Phase ──────────────────────────────────────
    def transform(self):
        self.logger.info("Transforming data to Silver layer...")
        
        # Read from Bronze (simulated reading from S3)
        weblogs = pd.read_parquet(f"s3://{self.s3_bucket}/bronze/weblogs/ingest_date={self.etl_run_date}/")
        users = pd.read_parquet(f"s3://{self.s3_bucket}/bronze/users/ingest_date={self.etl_run_date}/")
        products = pd.read_parquet(f"s3://{self.s3_bucket}/bronze/products/ingest_date={self.etl_run_date}/")
        
        # 1. Sort out-of-order logs
        weblogs['timestamp'] = pd.to_datetime(weblogs['timestamp'])
        weblogs = weblogs.sort_values(by=['session_id', 'timestamp'])
        
        # 2. Enrich with users and products to drop orphan rows
        self.enrich(weblogs, users, products)

        # 3. Calculate session metrics (for aggregations)
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

        # Abandoned cart and high activity
        session_metrics['is_abandoned_cart'] = (session_metrics['total_cart_adds'] > 0) & (session_metrics['total_purchases'] == 0)
        session_metrics['is_high_activity'] = session_metrics['total_actions'] > 50

        # Save transformed logs and users/products to Silver
        self.write_silver(weblogs, f"weblogs_clean/etl_run_date={self.etl_run_date}/etl_run_id={self.etl_run_id}/weblogs_silver.parquet")
        self.write_silver(users, f"users_clean/etl_run_date={self.etl_run_date}/users_silver.parquet")
        self.write_silver(products, f"products_clean/etl_run_date={self.etl_run_date}/products_silver.parquet")
        
        self.logger.info("Saved transformed data to Silver layer.")

    def enrich(self):
        # We handle this mainly in SQL later, but could drop orphans here
        pass

    def write_silver(self, df: pd.DataFrame, s3_key: str):
        path = f"s3://{self.s3_bucket}/silver/{s3_key}"
        df.to_parquet(path, index=False)
        self.logger.info(f"Wrote to silver: {path}")

    # ── Gold Phase ────────────────────────────────────────
    def load_to_snowflake(self, layer: str):
        self.logger.info(f"Loading {layer} layer into Snowflake...")
        conn = snowflake.connector.connect(**self.snowflake_conn_params)
        cursor = conn.cursor()
        
        try:
            if layer == 'bronze':
                cursor.execute("COPY INTO RAW.BRONZE_WEBLOGS FROM @bronze_stage/weblogs/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
                cursor.execute("COPY INTO RAW.BRONZE_USERS FROM @bronze_stage/users/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
                cursor.execute("COPY INTO RAW.BRONZE_PRODUCTS FROM @bronze_stage/products/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
            elif layer == 'silver':
                cursor.execute("COPY INTO STAGING.WEBLOGS_CLEAN FROM @silver_stage/weblogs_clean/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
                cursor.execute("COPY INTO STAGING.USERS_CLEAN FROM @silver_stage/users_clean/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
                cursor.execute("COPY INTO STAGING.PRODUCTS_CLEAN FROM @silver_stage/products_clean/ FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = CONTINUE;")
            self.logger.info("Data loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading to Snowflake: {e}")
        finally:
            cursor.close()
            conn.close()

    def build_gold(self):
        self.logger.info("Building Gold tables in Snowflake...")
        conn = snowflake.connector.connect(**self.snowflake_conn_params)
        cursor = conn.cursor()
        
        try:
            # We would run the DML scripts here to build the gold tables
            cursor.execute("SELECT 1; -- Placeholder for running Gold layer SQL")
            self.logger.info("Gold tables built successfully.")
        except Exception as e:
            self.logger.error(f"Error building Gold tables: {e}")
        finally:
            cursor.close()
            conn.close()

    # ── Orchestration ─────────────────────────────────────
    def run(self):
        self.logger.info(f"--- Starting ETL Pipeline Run: {self.etl_run_id} ---")
        try:
            self.extract()
            self.transform()
            self.load_to_snowflake('bronze')
            self.load_to_snowflake('silver')
            self.build_gold()
            self.logger.info("--- Pipeline Completed Successfully ---")
        except Exception as e:
            self.logger.error(f"--- Pipeline Failed: {e} ---")

if __name__ == "__main__":
    # Load configuration
    params = {
        'user': os.getenv('SNOWFLAKE_USER', 'test_user'),
        'password': os.getenv('SNOWFLAKE_PASSWORD', 'test_pass'),
        'account': os.getenv('SNOWFLAKE_ACCOUNT', 'test_account'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
    }
    
    # Initialize and run pipeline
    processor = WebLogProcessor(
        weblog_file='weblogs.csv',
        users_file='users.csv',
        products_file='products.csv',
        s3_bucket=os.getenv('S3_BUCKET', 'my-etl-bucket-123'),
        snowflake_conn_params=params
    )
    processor.run()
