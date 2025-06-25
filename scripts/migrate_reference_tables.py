"""
Database migration script to add is_active columns to reference tables.

This script:
1. Discovers all reference tables by naming convention and inheritance
2. Checks which tables are missing the is_active column
3. Adds the is_active column with default value True
4. Reports on migration status

Usage:
    python scripts/migrate_reference_tables.py [database_name]
"""

import importlib
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

from sqlalchemy import text
from sqlmodel import Session

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
    get_engine_from_param,
    discover_all_databases_with_tables,
)
from src._base.tables.table_type_mixins import get_table_type, validate_table_conventions

logger = logging.getLogger(__name__)


def discover_table_modules(src_path: str, database_name: str) -> List[tuple]:
    """
    Discover all table modules in a database directory.
    Returns list of (module, module_name) tuples.
    """
    tables_path = os.path.join(src_path, database_name, "tables")
    
    if not os.path.exists(tables_path):
        logger.warning(f"Tables directory not found: {tables_path}")
        return []

    if src_path not in sys.path:
        sys.path.insert(0, src_path)
    
    table_modules = []
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
                
                logger.debug(f"Attempting to import table module: {module_name}")
                try:
                    module = importlib.import_module(module_name)
                    table_modules.append((module, module_name))
                    logger.debug(f"Successfully imported table module: {module_name}")
                except ImportError as e:
                    logger.warning(f"Failed to import {module_name}: {e}")
                except Exception as e:
                    logger.error(f"Unexpected error importing {module_name}: {e}")
    
    return table_modules


def discover_reference_tables(src_path: str, database_name: str) -> List[tuple]:
    """
    Discover all reference tables by examining table classes.
    Returns list of (table_class, table_name, module_name) tuples.
    """
    table_modules = discover_table_modules(src_path, database_name)
    reference_tables = []
    
    for module, module_name in table_modules:
        # Look for SQLModel table classes in the module
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (hasattr(attr, "__table__") and 
                hasattr(attr, "__tablename__") and 
                attr.__table__ is not None):
                
                table_type = get_table_type(attr)
                if table_type == "reference":
                    reference_tables.append((attr, attr.__tablename__, module_name))
                    logger.info(f"Found reference table: {attr.__tablename__} in {module_name}")
    
    return reference_tables


def check_table_has_is_active(session: Session, schema_name: str, table_name: str) -> bool:
    """
    Check if a table already has an is_active column.
    """
    query = text("""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = :schema_name 
        AND table_name = :table_name 
        AND column_name = 'is_active'
    """)
    
    result = session.execute(query, {"schema_name": schema_name, "table_name": table_name})
    return result.first() is not None


def add_is_active_column(session: Session, schema_name: str, table_name: str) -> bool:
    """
    Add is_active column to a table with default value True.
    Returns True if successful, False otherwise.
    """
    try:
        # Add the column with default value
        alter_query = text(f"""
            ALTER TABLE {schema_name}.{table_name} 
            ADD COLUMN is_active BOOLEAN NOT NULL DEFAULT TRUE
        """)
        
        session.execute(alter_query)
        logger.info(f"✅ Added is_active column to {schema_name}.{table_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to add is_active column to {schema_name}.{table_name}: {e}")
        return False


def migrate_reference_tables_for_database(src_path: str, database_name: str) -> Dict[str, Any]:
    """
    Migrate all reference tables in a database to include is_active column.
    Returns summary of migration results.
    """
    logger.info(f"🚀 Starting reference table migration for database: {database_name}")
    
    # Discover reference tables
    reference_tables = discover_reference_tables(src_path, database_name)
    
    if not reference_tables:
        logger.info(f"No reference tables found for database {database_name}")
        return {"database": database_name, "tables_processed": 0, "migrations": []}
    
    # Connect to database
    engine = get_engine_from_param(
        DEFAULT_DB_ENGINE_TYPE, 
        database_name=database_name, 
        user_name=DEFAULT_USER_NAME, 
        host=DEFAULT_HOST, 
        port=DEFAULT_PORT
    )
    
    migration_results = []
    
    with Session(engine) as session:
        try:
            for table_class, table_name, module_name in reference_tables:
                # Extract schema from table_args
                schema_name = "public"  # default
                if hasattr(table_class, "__table_args__") and table_class.__table_args__:
                    if isinstance(table_class.__table_args__, dict):
                        schema_name = table_class.__table_args__.get("schema", "public")
                
                logger.info(f"📋 Processing {schema_name}.{table_name} from {module_name}")
                
                # Check if table already has is_active
                has_is_active = check_table_has_is_active(session, schema_name, table_name)
                
                if has_is_active:
                    logger.info(f"✅ {schema_name}.{table_name} already has is_active column")
                    migration_results.append({
                        "table": f"{schema_name}.{table_name}",
                        "module": module_name,
                        "action": "skipped",
                        "reason": "is_active column already exists",
                        "success": True
                    })
                else:
                    # Add is_active column
                    success = add_is_active_column(session, schema_name, table_name)
                    migration_results.append({
                        "table": f"{schema_name}.{table_name}",
                        "module": module_name,
                        "action": "added_is_active",
                        "success": success
                    })
            
            # Commit all changes
            session.commit()
            logger.info(f"✅ Reference table migration completed for database {database_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed during reference table migration for database {database_name}: {str(e)}")
            session.rollback()
            raise
    
    return {
        "database": database_name,
        "tables_processed": len(reference_tables),
        "migrations": migration_results
    }


def print_migration_summary(results: Dict[str, Any]):
    """Print a comprehensive summary of migration results."""
    logger.info("=" * 60)
    logger.info("📋 REFERENCE TABLE MIGRATION SUMMARY")
    logger.info("=" * 60)
    
    database = results["database"]
    migrations = results["migrations"]
    
    total_tables = len(migrations)
    successful_migrations = sum(1 for m in migrations if m["success"] and m["action"] == "added_is_active")
    skipped_tables = sum(1 for m in migrations if m["action"] == "skipped")
    failed_migrations = sum(1 for m in migrations if not m["success"])
    
    logger.info(f"🎯 Database: {database}")
    logger.info(f"📊 Tables processed: {total_tables}")
    logger.info(f"✅ Successful migrations: {successful_migrations}")
    logger.info(f"⏭️  Skipped (already migrated): {skipped_tables}")
    logger.info(f"❌ Failed migrations: {failed_migrations}")
    
    if migrations:
        logger.info("\n📋 Detailed Results:")
        for migration in migrations:
            status = "✅" if migration["success"] else "❌"
            action = migration["action"].replace("_", " ").title()
            table = migration["table"]
            
            if migration["action"] == "skipped":
                logger.info(f"  ⏭️  {table}: {migration['reason']}")
            else:
                logger.info(f"  {status} {table}: {action}")


if __name__ == "__main__":
    db_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DATABASE_NAME
    log_level = sys.argv[2].upper() if len(sys.argv) > 2 else DEFAULT_LOG_LEVEL

    # Configure logging
    configure_logging(log_level)
    
    logger.info("🚀 Starting reference table migration process")
    logger.info(f"📋 Target database: {db_name}")

    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

    try:
        if db_name and db_name != DEFAULT_DATABASE_NAME:
            # Migrate specific database
            results = migrate_reference_tables_for_database(src_dir, db_name)
        else:
            # Find demo database (our main target)
            db_name = "demo"
            results = migrate_reference_tables_for_database(src_dir, db_name)

        print_migration_summary(results)
        
        # Determine exit code
        failed_count = sum(1 for m in results["migrations"] if not m["success"])
        if failed_count > 0:
            logger.error(f"💥 {failed_count} migrations failed")
            sys.exit(1)
        else:
            logger.info("🎉 All migrations completed successfully!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"💥 Fatal error during migration: {e}")
        sys.exit(1)