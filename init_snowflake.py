import logging
import os
from pathlib import Path
from etl.config import load_settings
from etl.snowflake_loader import get_snowflake_loader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("init_snowflake")

def main():
    settings = load_settings()
    logger.info("Initializing Snowflake resources...")
    
    loader = get_snowflake_loader(settings)
    if not loader.conn_params:
        logger.error("No Snowflake credentials found! Check .env file.")
        return

    ddl_dir = Path("sql/ddl")
    ddl_files = sorted(ddl_dir.glob("*.sql"))

    import tempfile
    
    with loader as session:
        for ddl_file in ddl_files:
            logger.info(f"Executing {ddl_file.name}...")
            try:
                if ddl_file.name == "06_stages_and_file_formats.sql":
                    content = ddl_file.read_text()
                    content = content.replace("<bucket>", settings.s3_bucket_name)
                    content = content.replace("<aws_key_id>", settings.aws_access_key_id)
                    content = content.replace("<aws_secret_key>", settings.aws_secret_access_key)
                    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".sql") as tmp:
                        tmp.write(content)
                        tmp_path = tmp.name
                    results = session.run_sql_file(Path(tmp_path))
                    os.unlink(tmp_path)
                else:
                    results = session.run_sql_file(ddl_file)
                logger.info(f"Successfully executed {ddl_file.name}. Last result: {results[-1] if results else 'None'}")
            except Exception as e:
                logger.error(f"Failed to execute {ddl_file.name}: {e}")
                return

    logger.info("Initialization complete!")

if __name__ == "__main__":
    main()
