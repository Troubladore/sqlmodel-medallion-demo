import logging
import os
import sys
from pathlib import Path

from sqlalchemy import DDL
from sqlalchemy.engine import Engine

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from config import (
    DEFAULT_DATABASE_NAME,
    DEFAULT_DB_ENGINE_TYPE,
    DEFAULT_LOG_LEVEL,
    DEFAULT_USER_NAME,
    DEFAULT_HOST,
    DEFAULT_PORT,
)
from utils.utils import (
    configure_logging,
    discover_all_databases_with_schemas,
    discover_schemas_in_database,
    get_engine_from_param,
)

logger = logging.getLogger(__name__)


def create_schemas_for_database(engine: Engine, src_path: str, database_name: str) -> None:
    schemas = discover_schemas_in_database(src_path, database_name)

    if not schemas:
        logger.info(f"No schemas found for database {database_name}")
        return

    with engine.begin() as connection:
        for schema_obj, _ in schemas:
            schema_name = schema_obj.metadata.schema
            connection.execute(DDL(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            logger.info(f"Created schema '{schema_name}' for database {database_name}")


def create_schemas_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str) -> None:
    """Create schemas for all discovered databases"""
    databases = discover_all_databases_with_schemas(src_path)
    
    for database_name in databases:
        logger.info(f"Creating schemas for database: {database_name}")
        target_instance = get_engine_from_param(db_engine_type, database_name=database_name, user_name=user_name, host=host, port=port)
        create_schemas_for_database(target_instance, src_path, database_name)


if __name__ == "__main__":
    try:
        db_engine_type = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_ENGINE_TYPE
        db_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DATABASE_NAME
        log_level = sys.argv[3].upper() if len(sys.argv) > 3 else DEFAULT_LOG_LEVEL
        user_name = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_USER_NAME
        host = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_HOST
        port = int(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_PORT

        # Configure logging
        configure_logging(log_level)

        logger = logging.getLogger(__name__)
        logger.info("Starting schema creation process")
        logger.info(f"Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}, user_name={user_name}, host={host}, port={port}")

        src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

        if db_name:
            # Create schemas for specific database
            target_instance = get_engine_from_param(db_engine_type, database_name=db_name, user_name=user_name, host=host, port=port)
            create_schemas_for_database(target_instance, src_dir, db_name)
        else:
            # Create schemas for all discovered databases
            logger.info("No database name specified, creating schemas for all discovered databases")
            create_schemas_for_all_databases(db_engine_type, user_name, host, port, src_dir)

        logger.info("Schema creation process completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Fatal error during schema creation: {e}")
        logger.error(f"Schema creation process failed")
        sys.exit(1)
