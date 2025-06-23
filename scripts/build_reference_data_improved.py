import importlib
import logging
import os
import sys
from pathlib import Path
from typing import Optional, List, Dict

from sqlmodel import Session
from sqlalchemy.engine import Engine
from sqlalchemy.orm import configure_mappers

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


def discover_reference_data_modules(src_path: str, database_name: str) -> List[tuple]:
    """
    Discover reference data modules and return them with metadata for ordering
    Returns list of (module, priority, module_name) tuples
    """
    logger = logging.getLogger(__name__)
    data_path = os.path.join(src_path, database_name, "data")
    
    if not os.path.exists(data_path):
        logger.warning(f"Reference data directory not found: {data_path}")
        return []

    # Add parent directory to Python's path if needed
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
                    logger.info(f"Successfully imported reference data module: {module_name}")
                    
                    # Assign priority based on file name (status types should come before tables that reference them)
                    priority = 0
                    if "status_type" in filename:
                        priority = 1  # Load status types first
                    elif "type" in filename:
                        priority = 2  # Load other type tables second
                    else:
                        priority = 3  # Load everything else last
                    
                    data_modules.append((module, priority, module_name))
                    
                except ImportError as e:
                    logger.error(f"Failed to import {module_name}: {e}")
                    logger.error(f"Search path was: {sys.path}")
                    logger.warning(f"Could not import reference data module {module_name}. "
                                 f"Make sure the package structure is correct.")
                    # Continue processing other files instead of raising
                except Exception as e:
                    logger.error(f"Unexpected error importing {module_name}: {e}")
    
    # Sort by priority (lower numbers first)
    data_modules.sort(key=lambda x: x[1])
    return data_modules


def validate_reference_data_module(module, module_name: str) -> bool:
    """Validate that a module has the expected structure for reference data"""
    if not hasattr(module, "populate_reference_data"):
        logger.warning(f"Module {module_name} does not have populate_reference_data function")
        return False
    
    if not callable(getattr(module, "populate_reference_data")):
        logger.warning(f"Module {module_name} populate_reference_data is not callable")
        return False
    
    return True


def preload_table_models(src_path: str, database_name: str) -> None:
    """
    Pre-load all table models for a database to resolve SQLAlchemy relationships.
    This prevents circular import issues when reference data modules import models.
    """
    tables_path = os.path.join(src_path, database_name, "tables")
    
    if not os.path.exists(tables_path):
        logger.debug(f"Tables directory not found: {tables_path}")
        return
    
    # Add parent directory to Python's path if needed
    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    # Import all table modules to ensure SQLAlchemy relationships can be resolved
    for root, _, files in os.walk(tables_path):
        for filename in files:
            if filename.endswith(".py") and not filename.startswith("__"):
                # Calculate relative path from tables directory
                rel_path = os.path.relpath(root, tables_path)
                # Construct the module name using package structure
                if rel_path == ".":
                    module_name = f"src.{database_name}.tables.{filename[:-3]}"
                else:
                    module_name = f"src.{database_name}.tables.{rel_path.replace(os.sep, '.')}.{filename[:-3]}"
                
                try:
                    importlib.import_module(module_name)
                    logger.debug(f"Pre-loaded table model: {module_name}")
                except Exception as e:
                    logger.debug(f"Could not pre-load table model {module_name}: {e}")
    
    # Configure all mappers after all models are loaded to resolve circular relationships
    try:
        configure_mappers()
        logger.debug("Successfully configured all SQLAlchemy mappers")
    except Exception as e:
        logger.warning(f"Could not configure mappers: {e}")


def execute_reference_data_for_database(engine: Engine, src_path: str, database_name: str) -> Dict[str, bool]:
    """
    Execute reference data population for a specific database
    Returns dict of {module_name: success_status}
    """
    
    # Pre-load all table models to resolve SQLAlchemy relationships
    logger.debug(f"Pre-loading table models for database {database_name}")
    preload_table_models(src_path, database_name)
    
    # Discover reference data modules for this database
    data_modules = discover_reference_data_modules(src_path, database_name)
    
    if not data_modules:
        logger.info(f"No reference data found for database {database_name}")
        return {}

    results = {}
    
    with Session(engine) as session:
        try:
            for module, priority, module_name in data_modules:
                logger.info(f"Processing reference data module: {module_name} (priority: {priority})")
                
                if not validate_reference_data_module(module, module_name):
                    results[module_name] = False
                    continue
                
                try:
                    logger.info(f"Executing reference data population for {module_name}")
                    module.populate_reference_data(session)
                    logger.info(f"✅ Successfully populated reference data for {module_name}")
                    results[module_name] = True
                    
                except Exception as e:
                    logger.error(f"❌ Failed to execute {module_name}: {str(e)}")
                    results[module_name] = False
                    
                    # Check if we should continue on errors
                    if not os.getenv("CONTINUE_ON_ERROR", "").lower() == "true":
                        logger.error("Stopping due to error (set CONTINUE_ON_ERROR=true to continue)")
                        session.rollback()
                        raise
                    else:
                        logger.warning("Continuing despite error due to CONTINUE_ON_ERROR=true")
                        session.rollback()
                        # Start a new transaction for the next module
                        session.begin()
            
            # Commit all successful operations
            session.commit()
            logger.info(f"✅ Reference data population completed for database {database_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed during reference data population for database {database_name}: {str(e)}")
            session.rollback()
            raise
    
    return results


def execute_reference_data_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str) -> Dict[str, Dict[str, bool]]:
    """
    Execute reference data population for all discovered databases
    Returns dict of {database_name: {module_name: success_status}}
    """
    databases = discover_all_databases_with_reference_data(src_path)
    
    if not databases:
        logger.warning(f"No databases with reference data found in {src_path}")
        return {}
    
    all_results = {}
    
    for database_name in databases:
        logger.info(f"🚀 Starting reference data population for database: {database_name}")
        try:
            target_instance = get_engine_from_param(db_engine_type, database_name=database_name, user_name=user_name, host=host, port=port)
            results = execute_reference_data_for_database(target_instance, src_path, database_name)
            all_results[database_name] = results
            
            # Log summary for this database
            success_count = sum(1 for success in results.values() if success)
            total_count = len(results)
            logger.info(f"📊 Database {database_name}: {success_count}/{total_count} modules succeeded")
            
        except Exception as e:
            logger.error(f"❌ Failed to populate reference data for database {database_name}: {e}")
            all_results[database_name] = {}
            # Continue with other databases instead of failing completely
    
    return all_results


def discover_all_databases_with_reference_data(src_path):
    """Discover all database directories in src/ that have reference data"""
    logger = logging.getLogger(__name__)
    databases = []
    for item in os.listdir(src_path):
        item_path = os.path.join(src_path, item)
        if os.path.isdir(item_path) and not item.startswith('_'):
            data_path = os.path.join(item_path, "data")
            if os.path.exists(data_path):
                # Check if there are actual __data.py files
                has_data_files = False
                for root, _, files in os.walk(data_path):
                    if any(f.endswith("__data.py") for f in files):
                        has_data_files = True
                        break
                
                if has_data_files:
                    databases.append(item)
                    logger.debug(f"Found database with reference data: {item}")
                else:
                    logger.debug(f"Skipping {item} - data directory exists but no __data.py files found")
    return databases


def print_final_summary(all_results: Dict[str, Dict[str, bool]]):
    """Print a comprehensive summary of all operations"""
    logger.info("=" * 60)
    logger.info("📋 REFERENCE DATA POPULATION SUMMARY")
    logger.info("=" * 60)
    
    total_databases = len(all_results)
    total_modules = sum(len(db_results) for db_results in all_results.values())
    total_success = sum(
        sum(1 for success in db_results.values() if success) 
        for db_results in all_results.values()
    )
    
    logger.info(f"🎯 OVERALL: {total_success}/{total_modules} modules across {total_databases} databases")
    
    for database_name, db_results in all_results.items():
        if not db_results:
            logger.info(f"❌ {database_name}: Failed to process")
            continue
            
        success_count = sum(1 for success in db_results.values() if success)
        total_count = len(db_results)
        
        if success_count == total_count:
            logger.info(f"✅ {database_name}: {success_count}/{total_count} modules")
        else:
            logger.info(f"⚠️  {database_name}: {success_count}/{total_count} modules")
            
        # Show failed modules
        failed_modules = [name for name, success in db_results.items() if not success]
        if failed_modules:
            logger.info(f"   Failed modules: {', '.join(failed_modules)}")


if __name__ == "__main__":
    db_engine_type = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_ENGINE_TYPE
    db_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DATABASE_NAME
    log_level = sys.argv[3].upper() if len(sys.argv) > 3 else DEFAULT_LOG_LEVEL
    user_name = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_USER_NAME
    host = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_HOST
    port = int(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_PORT

    # Configure logging
    configure_logging(log_level)
    
    logger.info("🚀 Starting enhanced reference data population process")
    logger.info(f"📋 Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}")
    logger.info(f"📋 Connection: user={user_name}, host={host}, port={port}")

    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

    try:
        if db_name and db_name != DEFAULT_DATABASE_NAME:
            # Populate reference data for specific database
            logger.info(f"🎯 Targeting specific database: {db_name}")
            target_instance = get_engine_from_param(db_engine_type, database_name=db_name, user_name=user_name, host=host, port=port)
            results = execute_reference_data_for_database(target_instance, src_dir, db_name)
            all_results = {db_name: results}
        else:
            # Populate reference data for all discovered databases
            logger.info("🔍 Auto-discovering databases with reference data")
            all_results = execute_reference_data_for_all_databases(db_engine_type, user_name, host, port, src_dir)

        print_final_summary(all_results)
        
        # Determine exit code based on results
        total_success = sum(
            sum(1 for success in db_results.values() if success) 
            for db_results in all_results.values()
        )
        total_modules = sum(len(db_results) for db_results in all_results.values())
        
        if total_modules == 0:
            logger.info("✅ No reference data modules found - this is normal for most databases")
            sys.exit(0)
        elif total_success == total_modules:
            logger.info("🎉 All reference data populated successfully!")
            sys.exit(0)
        else:
            logger.error(f"💥 Some failures occurred: {total_success}/{total_modules} succeeded")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"💥 Fatal error during reference data population: {e}")
        sys.exit(1)