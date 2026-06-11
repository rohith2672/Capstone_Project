import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # AWS / S3 Settings
    STORAGE_BACKEND = os.getenv('STORAGE_BACKEND', 'local')
    LOCAL_LAKE_ROOT = os.getenv('LOCAL_LAKE_ROOT', 'data/lake')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # Snowflake Settings
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'ECOMMERCE_DW')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')

    # Pipeline Settings
    CHUNK_SIZE = int(os.getenv('CHUNK_SIZE', 10000))
    WEBLOG_FILE = os.getenv('WEBLOG_FILE', 'data/raw/weblogs.csv')
    USERS_FILE = os.getenv('USERS_FILE', 'data/raw/users.csv')
    PRODUCTS_FILE = os.getenv('PRODUCTS_FILE', 'data/raw/products.csv')
    DRY_RUN = os.getenv('DRY_RUN', 'true').lower() == 'true'

config = Config()
