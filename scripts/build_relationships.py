import logging
import os
import sys
from pathlib import Path

from sqlalchemy import inspect, Table
from sqlalchemy.schema import AddConstraint
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
    discover_tables_in_database,
    extract_table_classes_from_modules,
)
from engines.postgres_engine import get_engine as get_postgres_engine

logger = logging.getLogger(__name__)

"""
Foreign Key Relationship Management Script

IMPORTANT: SQLModel.metadata.create_all() creates tables but NOT foreign key constraints.
This script creates the FK constraints defined in your SQLModel classes.

Use Cases:
1. 🚀 Data Migrations: Drop FKs → Bulk load data → Recreate FKs → Validate integrity
2. 🔄 Schema Evolution: Add new FK relationships to existing tables  
3. 🏗️ Environment Setup: Ensure dev/staging/prod FK consistency
4. ⚡ Performance: Temporarily drop FKs for batch operations

Example workflow:
  # 1. Create tables (no FKs)
  python scripts/build_tables.py 
  
  # 2. Load large datasets (fast, no constraint checking)
  python scripts/load_data.py
  
  # 3. Create FK constraints (validates data integrity)
  python scripts/build_relationships.py
"""


def recreate_foreign_keys_for_database(engine: Engine, src_path: str, database_name: str, debug: bool = False) -> None:
    """Recreate foreign keys for a specific database"""
    # Use utility function to discover tables (returns modules, not classes)
    table_modules = discover_tables_in_database(src_path, database_name)
    
    if not table_modules:
        logger.info(f"No tables found for database {database_name}")
        return

    # Extract table classes from modules using utility function
    tables = extract_table_classes_from_modules(table_modules)
    
    if not tables:
        logger.warning(f"No table classes found in modules for database {database_name}")
        return
        
    inspector = inspect(engine)

    for table in tables:
        table_name = table.__tablename__
        # Get schema from table's __table_args__ if present
        schema = None
        if hasattr(table, '__table_args__'):
            table_args = table.__table_args__
            if isinstance(table_args, dict):
                schema = table_args.get('schema')
            elif isinstance(table_args, tuple) and len(table_args) > 0:
                # Check if the last element is a dict (common pattern: (constraints..., {'schema': 'name'}))
                if isinstance(table_args[-1], dict):
                    schema = table_args[-1].get('schema')
        
        # Check if table exists (with schema if specified)
        table_exists = inspector.has_table(table_name, schema=schema)
        schema_info = f" in schema '{schema}'" if schema else " (no schema specified)"
        
        if not table_exists:
            logger.warning(f"Table {table_name}{schema_info} does not exist in the database.")
            continue
        
        logger.info(f"✓ Found table {table_name}{schema_info} in database")

        # Get foreign keys (with schema if specified)
        existing_fks_info = inspector.get_foreign_keys(table_name, schema=schema)
        existing_fks = set()
        
        # Build a set of existing FK signatures (column -> referenced_table.referenced_column)
        for fk_info in existing_fks_info:
            for i, col in enumerate(fk_info['constrained_columns']):
                ref_table = fk_info['referred_table']
                ref_col = fk_info['referred_columns'][i]
                fk_signature = f"{col} -> {ref_table}.{ref_col}"
                existing_fks.add(fk_signature)

        if debug:
            logger.info(f"Discovered foreign keys in database for table {table_name}:")
            for fk in existing_fks_info:
                logger.info(f"  {fk['name']} -> {fk['referred_table']}({fk['referred_columns']})")

        try:
            # Autoload table with schema awareness
            table_obj = Table(table_name, table.metadata, autoload_with=engine, schema=schema)

            code_fks = []
            for fk in table_obj.foreign_key_constraints:
                if fk.elements:  # Ensure FK has elements
                    element = fk.elements[0]
                    local_col = element.parent.name
                    ref_table = element.column.table.name
                    ref_col = element.column.name
                    fk_signature = f"{local_col} -> {ref_table}.{ref_col}"
                    code_fks.append((fk, fk_signature))

            if debug and code_fks:
                logger.info(f"Defined foreign keys in code for table {table_name}:")
                for fk, signature in code_fks:
                    logger.info(f"  {signature}")

            # Only create FKs that don't already exist (by signature, not name)
            fks_to_create = [fk for fk, sig in code_fks if sig not in existing_fks]
            
            if not fks_to_create:
                logger.info(f"All foreign keys already exist for table {table_name}")
            
            for fk in fks_to_create:
                element = fk.elements[0]
                local_col = element.parent.name
                ref_table = element.column.table.name
                ref_col = element.column.name
                fk_desc = f"{local_col} -> {ref_table}.{ref_col}"
                
                logger.info(f"Creating foreign key: {fk_desc} on table {table_name}")
                try:
                    with engine.connect() as conn:
                        conn.execute(AddConstraint(fk))
                        conn.commit()
                        logger.info(f"✅ Successfully created foreign key: {fk_desc}")
                except Exception as e:
                    logger.error(f"Failed to create foreign key {fk_desc} on table {table_name}: {e}")
                    # Continue with other foreign keys instead of failing completely
        except Exception as e:
            logger.error(f"Failed to process table {table_name}: {e}")
            # Continue with other tables instead of failing completely
                    
    logger.info(f"Foreign key creation completed for database {database_name}")


def recreate_foreign_keys_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str, debug: bool = False) -> None:
    """Recreate foreign keys for all discovered databases"""
    databases = discover_all_databases_with_tables(src_path)
    
    if not databases:
        logger.warning(f"No databases with tables found in {src_path}")
        return
    
    for database_name in databases:
        logger.info(f"Recreating foreign keys for database: {database_name}")
        try:
            target_instance = get_postgres_engine(echo_msgs=False, database_name=database_name, user_name=user_name, host=host, port=port)
            recreate_foreign_keys_for_database(target_instance, src_path, database_name, debug)
        except Exception as e:
            logger.error(f"Failed to recreate foreign keys for database {database_name}: {e}")
            # Continue with other databases instead of failing completely


if __name__ == "__main__":
    db_engine_type = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_ENGINE_TYPE
    db_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DATABASE_NAME
    log_level = sys.argv[3].upper() if len(sys.argv) > 3 else DEFAULT_LOG_LEVEL
    user_name = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_USER_NAME
    host = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_HOST
    port = int(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_PORT
    debug_mode = bool(sys.argv[7]) if len(sys.argv) > 7 else True

    # Configure logging
    configure_logging(log_level)
    
    # Reduce SQLAlchemy logging noise unless in DEBUG mode
    if log_level.upper() != 'DEBUG':
        sqlalchemy_logger = logging.getLogger('sqlalchemy.engine')
        sqlalchemy_logger.setLevel(logging.WARNING)
    
    logger.info("Starting foreign key creation process")
    logger.info(f"Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}, user_name={user_name}, host={host}, port={port}, debug={debug_mode}")

    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

    if db_name:
        # Recreate foreign keys for specific database
        target_instance = get_postgres_engine(echo_msgs=False, database_name=db_name, user_name=user_name, host=host, port=port)
        recreate_foreign_keys_for_database(target_instance, src_dir, db_name, debug_mode)
    else:
        # Recreate foreign keys for all discovered databases
        logger.info("No database name specified, recreating foreign keys for all discovered databases")
        recreate_foreign_keys_for_all_databases(db_engine_type, user_name, host, port, src_dir, debug_mode)

    logger.info("Foreign key creation process completed")
