#!/usr/bin/env python3
"""
Standalone Trigger Validation Script

This script validates that temporal triggers are properly created and functional
across all databases. It can be run independently to check trigger health
without creating new triggers.

Critical for ensuring temporal schema integrity - if triggers aren't working,
your temporal tables won't track history, which is a silent data disaster.

Usage:
    # Validate triggers for all databases
    python scripts/validate_triggers.py
    
    # Validate triggers for specific database
    python scripts/validate_triggers.py postgres demo
    
    # With debug output
    python scripts/validate_triggers.py postgres demo DEBUG
"""

import os
import sys
from pathlib import Path

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
    print_temporal_discovery_summary
)
from src._base.trigger_validation import validate_triggers_for_database


def validate_triggers_for_single_database(engine_type: str, database_name: str, user_name: str, host: str, port: int, src_path: str, debug: bool = False) -> bool:
    """Validate triggers for a single database"""
    
    logger.info(f"🔍 Validating triggers for database: {database_name}")
    
    if debug:
        print_temporal_discovery_summary(src_path, database_name)
    
    # Discover temporal table pairs
    temporal_pairs = discover_temporal_tables_for_database(src_path, database_name)
    
    if not temporal_pairs:
        logger.info(f"No temporal table pairs found in database {database_name}")
        return True  # No temporal tables = no trigger validation needed
    
    logger.info(f"🎯 Found {len(temporal_pairs)} temporal table pairs to validate")
    
    # Get database engine
    engine = get_engine_from_param(
        engine_type, 
        database_name=database_name, 
        user_name=user_name, 
        host=host, 
        port=port
    )
    
    # Run comprehensive validation
    return validate_triggers_for_database(engine, temporal_pairs)


def validate_triggers_for_all_databases(engine_type: str, user_name: str, host: str, port: int, src_path: str, debug: bool = False) -> bool:
    """Validate triggers for all databases with temporal tables"""
    
    databases = discover_all_databases_with_tables(src_path)
    
    if not databases:
        logger.warning(f"No databases with tables found in {src_path}")
        return True
    
    logger.info(f"🌍 Discovered {len(databases)} databases: {', '.join(databases)}")
    
    all_valid = True
    validation_results = {}
    
    for database_name in databases:
        logger.info(f"🏗️  Validating database: {database_name}")
        try:
            is_valid = validate_triggers_for_single_database(
                engine_type, database_name, user_name, host, port, src_path, debug
            )
            validation_results[database_name] = is_valid
            if not is_valid:
                all_valid = False
                
        except Exception as e:
            logger.error(f"❌ Failed to validate database {database_name}: {e}")
            validation_results[database_name] = False
            all_valid = False
    
    # Print summary
    logger.info("=" * 80)
    logger.info("🎯 MULTI-DATABASE TRIGGER VALIDATION SUMMARY")
    logger.info("=" * 80)
    
    for database_name, is_valid in validation_results.items():
        status = "✅" if is_valid else "❌"
        logger.info(f"{status} {database_name}: {'VALID' if is_valid else 'INVALID'}")
    
    valid_count = sum(1 for is_valid in validation_results.values() if is_valid)
    total_count = len(validation_results)
    
    if all_valid:
        logger.info(f"🎉 SUCCESS: All {total_count} databases have valid temporal triggers!")
    else:
        logger.error(f"💥 FAILURE: {total_count - valid_count}/{total_count} databases have trigger issues!")
        logger.error("   Temporal schema integrity is compromised - history tracking may not work")
    
    logger.info("=" * 80)
    
    return all_valid


if __name__ == "__main__":
    import logging
    
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
    
    logger.info("🔍 Starting comprehensive temporal trigger validation")
    logger.info(f"📋 Parameters: engine_type={db_engine_type}, db_name={db_name}, log_level={log_level}")
    logger.info(f"📋 Connection: user={user_name}, host={host}, port={port}, debug={debug_mode}")

    src_dir = os.path.join(str(Path(__file__).parent.parent), "src")

    try:
        if db_name and db_name != DEFAULT_DATABASE_NAME:
            # Validate triggers for specific database
            logger.info(f"🎯 Targeting specific database: {db_name}")
            success = validate_triggers_for_single_database(
                db_engine_type, db_name, user_name, host, port, src_dir, debug_mode
            )
        else:
            # Validate triggers for all discovered databases
            logger.info("🔍 Auto-discovering databases with temporal tables")
            success = validate_triggers_for_all_databases(
                db_engine_type, user_name, host, port, src_dir, debug_mode
            )

        if success:
            logger.info("🎉 Temporal trigger validation completed successfully!")
            logger.info("✅ All temporal schemas are operational and will track history correctly")
            sys.exit(0)
        else:
            logger.error("💥 Temporal trigger validation FAILED!")
            logger.error("❌ Temporal schema integrity is compromised")
            logger.error("🔧 Recommendation: Re-run build_triggers.py to fix issues")
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"💥 Fatal error during trigger validation: {e}")
        sys.exit(1)