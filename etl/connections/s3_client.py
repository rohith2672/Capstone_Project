import boto3
import os
import io
import pandas as pd
import logging
from etl.config import config

logger = logging.getLogger(__name__)

class StorageClient:
    """Handles read/write operations to Storage (S3 or Local)"""
    def __init__(self):
        self.backend = config.STORAGE_BACKEND
        self.bucket = config.S3_BUCKET_NAME
        self.local_root = config.LOCAL_LAKE_ROOT
        
        if self.backend == 's3':
            self.s3 = boto3.client('s3', region_name=config.AWS_REGION)
            logger.info(f"Initialized S3 StorageClient for bucket: {self.bucket}")
        else:
            self.s3 = None
            logger.info(f"Initialized Local StorageClient at: {self.local_root}")

    def write_parquet(self, df: pd.DataFrame, key: str):
        if self.backend == 's3':
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
            logger.info(f"Wrote to S3: s3://{self.bucket}/{key}")
        else:
            path = os.path.join(self.local_root, key)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            df.to_parquet(path, index=False)
            logger.info(f"Wrote to Local: {path}")

    def read_parquet(self, key_prefix: str) -> pd.DataFrame:
        if self.backend == 's3':
            # Simplified read using pandas S3 integration if s3fs was installed,
            # but we use boto3 directly to download then read, avoiding s3fs requirement.
            objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=key_prefix)
            dfs = []
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        response = self.s3.get_object(Bucket=self.bucket, Key=obj['Key'])
                        dfs.append(pd.read_parquet(io.BytesIO(response['Body'].read())))
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        else:
            path = os.path.join(self.local_root, key_prefix)
            if not os.path.exists(path):
                return pd.DataFrame()
            if os.path.isdir(path):
                files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.parquet')]
                dfs = [pd.read_parquet(f) for f in files]
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
            else:
                return pd.read_parquet(path)
