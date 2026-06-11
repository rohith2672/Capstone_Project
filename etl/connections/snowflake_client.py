import snowflake.connector
import logging
from etl.config import config

logger = logging.getLogger(__name__)

class SnowflakeClient:
    """Handles connection and queries to Snowflake"""
    def __init__(self):
        self.dry_run = config.DRY_RUN
        self.params = {
            'user': config.SNOWFLAKE_USER,
            'password': config.SNOWFLAKE_PASSWORD,
            'account': config.SNOWFLAKE_ACCOUNT,
            'role': config.SNOWFLAKE_ROLE,
            'warehouse': config.SNOWFLAKE_WAREHOUSE,
            'database': config.SNOWFLAKE_DATABASE,
            'schema': config.SNOWFLAKE_SCHEMA
        }

    def execute(self, query: str):
        if self.dry_run:
            logger.info(f"[DRY RUN] Skipping query execution:\n{query[:100]}...")
            return

        logger.info(f"Executing query on Snowflake...")
        conn = snowflake.connector.connect(**self.params)
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            logger.info("Query executed successfully.")
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    def run_script(self, script_path: str):
        if self.dry_run:
            logger.info(f"[DRY RUN] Skipping script execution: {script_path}")
            return
            
        with open(script_path, 'r') as file:
            sql = file.read()
            import re
            # Remove single line comments to avoid empty statement errors
            sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
            # Split by statement (simplified, assuming standard formatting)
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            for stmt in statements:
                if stmt:
                    self.execute(stmt + ';')
