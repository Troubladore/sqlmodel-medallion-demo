import logging
import os
import sys
from pathlib import Path

from sqlalchemy.engine import Engine
from sqlmodel import SQLModel

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
    discover_all_databases_with_tables,
    discover_functions_in_database,
    discover_tables_in_database,
    get_engine_from_param,
)

logger = logging.getLogger(__name__)


def create_tables_for_database(engine: Engine, src_path: str, database_name: str) -> None:
    from utils.utils import discover_database_tables
    
    # Discover and register table classes for this database only
    table_classes = discover_database_tables(src_path, database_name, return_type='classes')
    
    if not table_classes:
        logger.info(f"No tables found for database {database_name}")
        return

    logger.info(f"Found {len(table_classes)} table classes for database {database_name}")

    # Discover and attach SQL functions to metadata for this database
    discover_functions_in_database(src_path, database_name)

    # Create a filtered list of tables that belong only to this database
    # We'll create them by filtering SQLModel.metadata.tables
    tables_to_create = []
    for table_class in table_classes:
        if hasattr(table_class, '__table__'):
            tables_to_create.append(table_class.__table__)
            logger.debug(f"Will create table: {table_class.__tablename__}")

    if tables_to_create:
        # Create only the tables that belong to this database
        SQLModel.metadata.create_all(engine, tables=tables_to_create)
        logger.info(f"Created {len(tables_to_create)} tables for database {database_name}")
    else:
        logger.warning(f"No valid tables found to create for database {database_name}")


def create_tables_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str) -> None:
    """Create tables for all discovered databases"""
    databases = discover_all_databases_with_tables(src_path)
    
    for database_name in databases:
        logger.info(f"Creating tables for database: {database_name}")
        target_instance = get_engine_from_param(db_engine_type, database_name=database_name, user_name=user_name, host=host, port=port)
        create_tables_for_database(target_instance, src_path, database_name)


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
        logger.info("Starting table creation process")
        logger.info(f"Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}, user_name={user_name}, host={host}, port={port}")

        src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

        if db_name:
            # Create tables for specific database
            target_instance = get_engine_from_param(db_engine_type, database_name=db_name, user_name=user_name, host=host, port=port)
            create_tables_for_database(target_instance, src_dir, db_name)
        else:
            # Create tables for all discovered databases
            logger.info("No database name specified, creating tables for all discovered databases")
            create_tables_for_all_databases(db_engine_type, user_name, host, port, src_dir)

        logger.info("Table creation process completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Fatal error during table creation: {e}")
        logger.error(f"Table creation process failed")
        sys.exit(1)
