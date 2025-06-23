"""
Enhanced Temporal Trigger Builder with Auto-Discovery

This script automatically discovers temporal table pairs (primary → __history)
and generates the appropriate triggers without manual configuration.

Key Features:
- Auto-discovers temporal table pairs using naming conventions
- Validates that history tables contain all primary table columns
- Generates triggers based on table structure analysis
- Provides comprehensive error reporting and validation

Usage:
    # Auto-generate triggers for all temporal tables in all databases
    python scripts/build_triggers.py
    
    # Auto-generate triggers for specific database
    python scripts/build_triggers.py postgres demo
    
    # With debug mode for detailed discovery information
    python scripts/build_triggers.py postgres demo DEBUG
"""

import logging
import os
import sys
from typing import List
from pathlib import Path

from sqlalchemy.engine import Engine
from sqlalchemy import DDL, text

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
    discover_all_databases_with_tables
)
from src._base.temporal_discovery import (
    discover_temporal_tables_for_database,
    generate_trigger_config,
    analyze_temporal_table_pair,
    print_temporal_discovery_summary
)
from src._base.trigger_validation import validate_triggers_for_database

logger = logging.getLogger(__name__)


def drop_existing_triggers_and_functions(
    connection,
    schema_name: str,
    table_name: str,
) -> None:
    """Drop existing triggers and functions if they exist."""
    # Drop triggers if they exist
    for trigger_type in ['insert', 'update', 'delete']:
        trigger_name = f"trg_{table_name}_{trigger_type}"
        connection.execute(
            text(f"""
                DROP TRIGGER IF EXISTS {trigger_name}
                ON {schema_name}.{table_name}
            """)
        )
        
        # Drop trigger functions if they exist
        connection.execute(
            text(f"""
                DROP FUNCTION IF EXISTS {trigger_name}()
            """)
        )


def create_triggers(
    engine: Engine,
    schema_name: str,
    table_name: str,
    history_table_name: str,
    key_column: str,
    additional_columns: List[str],
) -> None:


    # Format columns for history table insert
    history_columns = ", ".join(
        [key_column]
        + additional_columns
        + ["effective_time", "systime", "operation_type"]
    )

    # Define the function for calculate_systime_range
    calculate_systime_range_ddl = DDL(
        """
        CREATE OR REPLACE FUNCTION calculate_systime_range(start_time TIMESTAMP WITHOUT TIME ZONE, end_time TIMESTAMP WITHOUT TIME ZONE)
        RETURNS tsrange AS $$
        BEGIN
            IF end_time IS NULL THEN
                RETURN tsrange(start_time, 'infinity', '[)');
            ELSE
                RETURN tsrange(start_time, end_time, '[)');
            END IF;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # Define the insert trigger function
    insert_trigger_function_ddl = DDL(
        f"""
        CREATE OR REPLACE FUNCTION trg_{table_name}_insert()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.effective_time IS NULL THEN
                NEW.effective_time := now();
            END IF;

            INSERT INTO {schema_name}.{history_table_name} (
                history_id,
                {history_columns}
            )
            VALUES (
                gen_random_uuid(),
                NEW.{key_column},
                {', '.join([f'NEW.{col}' for col in additional_columns])},
                tsrange(NEW.effective_time, 'infinity', '[)'),
                calculate_systime_range(NEW.effective_time, NULL),
                '1'
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # Define the insert trigger
    insert_trigger_ddl = DDL(
        f"""
        CREATE TRIGGER trg_{table_name}_insert
        AFTER INSERT ON {schema_name}.{table_name}
        FOR EACH ROW
        EXECUTE FUNCTION trg_{table_name}_insert();
        """
    )

    # Define the delete trigger function
    delete_trigger_function_ddl = DDL(
        f"""
        CREATE OR REPLACE FUNCTION trg_{table_name}_delete()
        RETURNS TRIGGER AS $$
        DECLARE
            current_time_utc TIMESTAMP;
        BEGIN
            current_time_utc := now();

            UPDATE {schema_name}.{history_table_name}
            SET 
                effective_time = tsrange(OLD.effective_time, current_time_utc, '[)'),
                systime = tsrange(lower(systime), current_time_utc, '[)'),
                operation_type = NULL
            WHERE 
                {key_column} = OLD.{key_column}
                AND systime = tsrange(lower(systime), 'infinity', '[)')
                AND effective_time = tsrange(OLD.effective_time, 'infinity', '[)');

            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # Define the delete trigger
    delete_trigger_ddl = DDL(
        f"""
        CREATE TRIGGER trg_{table_name}_delete
        AFTER DELETE ON {schema_name}.{table_name}
        FOR EACH ROW
        EXECUTE FUNCTION trg_{table_name}_delete();
        """
    )

    # Define the update trigger function
    update_trigger_function_ddl = DDL(
        f"""
        CREATE OR REPLACE FUNCTION trg_{table_name}_update()
        RETURNS TRIGGER AS $$
        DECLARE
            systime_range TSRANGE;
        BEGIN
            IF NEW.effective_time IS NULL THEN
                NEW.effective_time := now();
            END IF;

            systime_range := calculate_systime_range(NEW.effective_time, now() AT TIME ZONE 'UTC');

            UPDATE {schema_name}.{history_table_name}
            SET 
                effective_time = tsrange(OLD.effective_time, NEW.effective_time, '[)'),
                systime = tsrange(lower(systime), lower(systime_range), '[)')
            WHERE 
                {key_column} = OLD.{key_column}
                AND systime = tsrange(lower(systime), 'infinity', '[)')
                AND effective_time = tsrange(OLD.effective_time, 'infinity', '[)');

            INSERT INTO {schema_name}.{history_table_name} (
                history_id, 
                {history_columns}
            )
            VALUES (
                gen_random_uuid(),
                NEW.{key_column},
                {', '.join([f'NEW.{col}' for col in additional_columns])},
                tsrange(NEW.effective_time, 'infinity', '[)'),
                tsrange(lower(systime_range), 'infinity', '[)'),
                '0'
            );

            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )

    # Define the update trigger
    update_trigger_ddl = DDL(
        f"""
        CREATE TRIGGER trg_{table_name}_update
        AFTER UPDATE ON {schema_name}.{table_name}
        FOR EACH ROW
        EXECUTE FUNCTION trg_{table_name}_update();
        """
    )

    # Bind DDL events to engine
    with engine.connect() as connection:
        try:
            connection.begin()
            
            # First, drop any existing triggers and functions
            drop_existing_triggers_and_functions(connection, schema_name, table_name)
            
            # Create calculate_systime_range function
            connection.execute(calculate_systime_range_ddl)

            # Create insert trigger and function
            connection.execute(insert_trigger_function_ddl)
            connection.execute(insert_trigger_ddl)

            # Create delete trigger and function
            connection.execute(delete_trigger_function_ddl)
            connection.execute(delete_trigger_ddl)

            # Create update trigger and function
            connection.execute(update_trigger_function_ddl)
            connection.execute(update_trigger_ddl)

            # Commit the transaction to ensure triggers and functions are created
            connection.commit()

            logger.info(f"Triggers and functions for {table_name} created successfully.")

        except Exception as e:
            connection.rollback()
            logger.error(f"Error creating triggers for {table_name}: {str(e)}")
            raise


def create_triggers_for_database(engine: Engine, src_path: str, database_name: str, debug: bool = False) -> None:
    """
    Auto-discover and create triggers for all temporal tables in a database.
    
    This replaces the hardcoded approach with elegant auto-discovery.
    """
    logger.info(f"🚀 Auto-discovering temporal tables for database: {database_name}")
    
    if debug:
        print_temporal_discovery_summary(src_path, database_name)
    
    # Discover all valid temporal table pairs
    temporal_pairs = discover_temporal_tables_for_database(src_path, database_name)
    
    if not temporal_pairs:
        logger.info(f"No temporal table pairs found in database {database_name}")
        return
    
    logger.info(f"🎯 Found {len(temporal_pairs)} temporal table pairs ready for trigger generation")
    
    # Generate triggers for each valid pair
    trigger_creation_success = True
    for pair in temporal_pairs:
        logger.info(f"🔧 Processing temporal pair: {pair.base_name}")
        
        # Analyze the pair to get trigger configuration
        analysis = analyze_temporal_table_pair(pair)
        config = generate_trigger_config(analysis)
        
        logger.info(f"   Primary table: {config.table_name}")
        logger.info(f"   History table: {config.history_table_name}")
        logger.info(f"   Schema: {config.schema_name or 'default'}")
        logger.info(f"   Primary key: {config.key_column}")
        logger.info(f"   Trigger columns: {', '.join(config.additional_columns)}")
        
        try:
            # Create triggers using the existing function
            create_triggers(
                engine=engine,
                schema_name=config.schema_name or "public",
                table_name=config.table_name,
                history_table_name=config.history_table_name,
                key_column=config.key_column,
                additional_columns=config.additional_columns
            )
            logger.info(f"✅ Successfully created triggers for {config.table_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to create triggers for {config.table_name}: {e}")
            trigger_creation_success = False
            # Continue with other tables instead of failing completely
    
    # CRITICAL: Validate that all triggers are actually working
    logger.info(f"🔍 Running comprehensive trigger validation for database {database_name}")
    validation_success = validate_triggers_for_database(engine, temporal_pairs)
    
    if not validation_success:
        logger.error(f"💥 VALIDATION FAILED: Triggers for database {database_name} are not working properly!")
        logger.error("   This is a critical failure - temporal schema will not track history correctly")
        raise RuntimeError(f"Trigger validation failed for database {database_name}")
    
    if not trigger_creation_success:
        logger.error(f"💥 Some trigger creations failed for database {database_name}")
        raise RuntimeError(f"Trigger creation failures in database {database_name}")
    
    logger.info(f"🎉 All triggers validated successfully for database {database_name}")
    logger.info(f"✅ Temporal schema is fully operational and will track history correctly")


def create_triggers_for_all_databases(db_engine_type: str, user_name: str, host: str, port: int, src_path: str, debug: bool = False) -> None:
    """Auto-discover and create triggers for all databases with temporal tables"""
    
    databases = discover_all_databases_with_tables(src_path)
    
    if not databases:
        logger.warning(f"No databases with tables found in {src_path}")
        return
    
    logger.info(f"🌍 Discovered {len(databases)} databases: {', '.join(databases)}")
    
    for database_name in databases:
        logger.info(f"🏗️  Processing database: {database_name}")
        try:
            target_instance = get_engine_from_param(
                db_engine_type, 
                database_name=database_name, 
                user_name=user_name, 
                host=host, 
                port=port
            )
            create_triggers_for_database(target_instance, src_path, database_name, debug)
            
        except Exception as e:
            logger.error(f"❌ Failed to process database {database_name}: {e}")
            # Continue with other databases instead of failing completely


if __name__ == "__main__":
    db_engine_type = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DB_ENGINE_TYPE
    db_name = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_DATABASE_NAME
    log_level = sys.argv[3].upper() if len(sys.argv) > 3 else DEFAULT_LOG_LEVEL
    user_name = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_USER_NAME
    host = sys.argv[5] if len(sys.argv) > 5 else DEFAULT_HOST
    port = int(sys.argv[6]) if len(sys.argv) > 6 else DEFAULT_PORT
    debug_mode = bool(sys.argv[7]) if len(sys.argv) > 7 else False

    # Configure logging
    configure_logging(log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting enhanced temporal trigger creation with auto-discovery")
    logger.info(f"📋 Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}")
    logger.info(f"📋 Connection: user={user_name}, host={host}, port={port}, debug={debug_mode}")

    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

    try:
        if db_name and db_name != DEFAULT_DATABASE_NAME:
            # Create triggers for specific database
            logger.info(f"🎯 Targeting specific database: {db_name}")
            target_instance = get_engine_from_param(db_engine_type, database_name=db_name, user_name=user_name, host=host, port=port)
            create_triggers_for_database(target_instance, src_dir, db_name, debug_mode)
        else:
            # Create triggers for all discovered databases
            logger.info("🔍 Auto-discovering databases with temporal tables")
            create_triggers_for_all_databases(db_engine_type, user_name, host, port, src_dir, debug_mode)

        logger.info("🎉 Enhanced temporal trigger creation completed successfully!")
        sys.exit(0)
        
    except Exception as e:
        logger.error(f"💥 Fatal error during trigger creation: {e}")
        sys.exit(1)
