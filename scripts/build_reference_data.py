import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Optional

from sqlmodel import Session
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
    discover_all_databases_with_tables,
    get_engine_from_param,
)

logger = logging.getLogger(__name__)


def discover_reference_data_in_database(src_path: str, database_name: str):
    """Discover reference data in a specific database following the same pattern as table discovery"""
    logger = logging.getLogger(__name__)
    data_path = os.path.join(src_path, database_name, "data")
    
    if not os.path.exists(data_path):
        logger.warning(f"Reference data directory not found: {data_path}")
        return []

    # Add parent directory to Python's path if needed (improves import reliability)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    data_modules = []
    for root, _, files in os.walk(data_path):
        for filename in files:
            if filename.endswith("__data.py"):
                # Calculate relative path from data directory
                rel_path = os.path.relpath(root, data_path)
                # Construct the module name using package structure
                if rel_path == ".":
                    module_name = f"src.{database_name}.data.{filename[:-3]}"
                else:
                    module_name = f"src.{database_name}.data.{rel_path.replace(os.sep, '.')}.{filename[:-3]}"
                
                logger.debug(f"Attempting to import reference data module: {module_name}")
                try:
                    module = importlib.import_module(module_name)
                    logger.info(f"Importing reference data module: {module_name}")
                    data_modules.append(module)
                except ImportError as e:
                    logger.error(f"Failed to import {module_name}: {e}")
                    logger.error(f"Search path was: {sys.path}")
                    logger.warning(f"Could not import reference data module {module_name}. "
                                 f"Make sure the package structure is correct and "
                                 f"the parent directory '{src_path}' contains a valid Python package.")
                    # Continue processing other files instead of raising
    return data_modules


def discover_all_databases_with_reference_data(src_path):
    """Discover all database directories in src/ that have reference data"""
    logger = logging.getLogger(__name__)
    databases = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path) and not item.startswith('_'):
            data_path = os.path.join(item_path, "data")
            if os.path.exists(data_path):
                databases.append(item)
                logger.debug(f"Found database with reference data: {item}")
    return databases


def execute_reference_data_for_database(engine: Engine, src_path: str, database_name: str) -> None:
    """Execute reference data population for a specific database"""
    
    # Discover reference data modules for this database
    data_modules = discover_reference_data_in_database(src_path, database_name)
    
    if not data_modules:
        logger.info(f"No reference data found for database {database_name}")
        return

    with Session(engine) as session:
        try:
            for module in data_modules:
                if hasattr(module, "populate_reference_data"):
                    module_name = getattr(module, '__name__', 'unknown_module')
                    logger.info(f"Executing reference data population for {module_name}")
                    try:
                        module.populate_reference_data(session)
                    except Exception as e:
                        logger.error(f"Failed to execute {module_name}: {str(e)}")
                        if not os.getenv("CONTINUE_ON_ERROR", "").lower() == "true":
                            raise
                        session.rollback()
                else:
                    module_name = getattr(module, '__name__', 'unknown_module')
                    logger.warning(f"Module {module_name} does not have populate_reference_data function")
            
            session.commit()
            logger.info(f"Reference data population completed for database {database_name}")
        except Exception as e:
            logger.error(f"Failed during reference data population for database {database_name}: {str(e)}")
            session.rollback()
            raise


def execute_reference_data_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str) -> None:
    """Execute reference data population for all discovered databases"""
    databases = discover_all_databases_with_reference_data(src_path)
    
    if not databases:
        logger.warning(f"No databases with reference data found in {src_path}")
        return
    
    for database_name in databases:
        logger.info(f"Populating reference data for database: {database_name}")
        try:
            target_instance = get_engine_from_param(db_engine_type, database_name=database_name, user_name=user_name, host=host, port=port)
            execute_reference_data_for_database(target_instance, src_path, database_name)
        except Exception as e:
            logger.error(f"Failed to populate reference data for database {database_name}: {e}")
            # Continue with other databases instead of failing completely


if __name__ == "__main__":
    db_engine_type = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_ENGINE_TYPE
    db_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DATABASE_NAME
    log_level = sys.argv[3].upper() if len(sys.argv) > 3 else DEFAULT_LOG_LEVEL
    user_name = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_USER_NAME
    host = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_HOST
    port = int(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_PORT

    # Configure logging
    configure_logging(log_level)
    
    logger.info("Starting reference data population process")
    logger.info(f"Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}, user_name={user_name}, host={host}, port={port}")

    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

    if db_name:
        # Populate reference data for specific database
        target_instance = get_engine_from_param(db_engine_type, database_name=db_name, user_name=user_name, host=host, port=port)
        execute_reference_data_for_database(target_instance, src_dir, db_name)
    else:
        # Populate reference data for all discovered databases
        logger.info("No database name specified, populating reference data for all discovered databases")
        execute_reference_data_for_all_databases(db_engine_type, user_name, host, port, src_dir)

    logger.info("Reference data population process completed")
