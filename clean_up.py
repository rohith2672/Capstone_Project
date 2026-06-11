import boto3
from etl.config import load_settings
settings = load_settings()
import snowflake.connector

print("Cleaning S3 bucket...")
s3 = boto3.resource('s3', region_name=settings.aws_region, aws_access_key_id=settings.aws_access_key_id, aws_secret_access_key=settings.aws_secret_access_key)
bucket = s3.Bucket(settings.s3_bucket_name)
bucket.objects.all().delete()
print(f"Cleaned S3 bucket {settings.s3_bucket_name}")

print("Cleaning Snowflake...")
conn = snowflake.connector.connect(
    user=settings.snowflake_user,
    password=settings.snowflake_password,
    account=settings.snowflake_account,
    warehouse=settings.snowflake_warehouse,
    role=settings.snowflake_role
)
conn.cursor().execute("DROP DATABASE IF EXISTS ECOMMERCE_DW")
print("Dropped Snowflake Database ECOMMERCE_DW")
