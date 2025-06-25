import logging
import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy.engine import Engine

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
    create_database_function_for_name,
    discover_databases_with_functions,
    get_engine_from_param,
)

logger = logging.getLogger(__name__)


def create_or_update_databases(engine: Engine, src_path: str) -> None:
    databases = discover_databases_with_functions(src_path)

    for create_db, db_name in databases:
        create_db(engine)
        logger.info(f"Executed create_target_db for database: {db_name}")


def create_specific_database(engine: Engine, src_path: str, target_db_name: str) -> None:
    # Look for the specific database script
    target_script_path = os.path.join(src_path, target_db_name, f"{target_db_name}.py")
    if not os.path.exists(target_script_path):
        error_msg = f"Database script not found: {target_script_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # Use utility function to get the create function
    create_function = create_database_function_for_name(src_path, target_db_name)
    if create_function:
        create_function(engine)
        logger.info(f"Executed create_target_db for {target_db_name}")
    else:
        error_msg = f"Could not find or load create_target_db function for {target_db_name}"
        logger.error(error_msg)
        raise ImportError(error_msg)


def create_or_update_all_databases(db_engine_type: str, log_level: str, user_name: str, host: str, port: int, src_path: str) -> None:
    databases = discover_databases_with_functions(src_path)
    
    for create_db, db_name in databases:
        # Connect to the postgres database to create each target database
        postgres_engine = get_engine_from_param(db_engine_type, database_name="postgres", user_name=user_name, host=host, port=port)
        create_db(postgres_engine)
        logger.info(f"Executed create_target_db for database: {db_name}")


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

        logger.info(f"Starting database creation/update process")
        logger.info(f"Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}, user_name={user_name}, host={host}, port={port}")

        src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

        if db_name:
            # Create specific database
            target_instance = get_engine_from_param(db_engine_type, database_name="postgres", user_name=user_name, host=host, port=port)
            create_specific_database(target_instance, src_dir, db_name)
        else:
            # Create all discovered databases
            logger.info("No database name specified, creating all discovered databases")
            create_or_update_all_databases(db_engine_type, log_level, user_name, host, port, src_dir)

        logger.info("Database creation/update process completed successfully")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"Fatal error during database creation: {e}")
        logger.error(f"Database creation process failed")
        sys.exit(1)
