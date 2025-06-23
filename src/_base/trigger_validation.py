"""
Comprehensive Trigger Validation System

This module provides robust validation to ensure temporal triggers are properly
created and functional. This is critical because silent trigger failures would
result in temporal tables that don't track history - a silent data integrity disaster.

Validation Levels:
1. Static Validation: Do triggers exist in the database?
2. Function Validation: Do trigger functions exist and compile?
3. Coverage Validation: Does each temporal table have complete trigger set?
4. Functional Validation: Do triggers actually fire and create history records?

Usage:
    validator = TriggerValidator(engine)
    validation_report = validator.validate_all_triggers(temporal_pairs)
    if validation_report.all_valid:
        print("✅ All temporal triggers validated successfully!")
    else:
        print("❌ Trigger validation failures detected!")
"""

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Dict, List, Optional, Set, Any, Tuple
from sqlalchemy.engine import Engine
from sqlalchemy import text
from contextlib import contextmanager

from src._base.temporal_discovery import TemporalTablePair, TriggerConfig

logger = logging.getLogger(__name__)


@dataclass
class TriggerInfo:
    """Information about a database trigger"""
    trigger_name: str
    table_name: str
    schema_name: str
    event_manipulation: str  # INSERT, UPDATE, DELETE
    action_timing: str       # BEFORE, AFTER
    exists: bool


@dataclass
class FunctionInfo:
    """Information about a trigger function"""
    function_name: str
    schema_name: str
    exists: bool
    is_valid: bool


@dataclass
class TemporalTableValidation:
    """Validation results for a single temporal table"""
    table_name: str
    schema_name: str
    expected_triggers: List[str]
    actual_triggers: List[TriggerInfo]
    missing_triggers: List[str]
    trigger_functions: List[FunctionInfo]
    missing_functions: List[str]
    functional_test_passed: bool
    functional_test_error: Optional[str]
    is_valid: bool
    issues: List[str]


@dataclass
class TriggerValidationReport:
    """Overall validation report for all temporal tables"""
    table_validations: List[TemporalTableValidation]
    total_tables: int
    valid_tables: int
    invalid_tables: int
    all_valid: bool
    summary_issues: List[str]


class TriggerValidator:
    """
    Comprehensive trigger validation system for temporal tables.
    
    Validates that temporal triggers exist, are functional, and provide
    complete coverage for all temporal table pairs.
    """
    
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def get_database_triggers(self, schema_name: str, table_name: str) -> List[TriggerInfo]:
        """
        Query the database to get all triggers for a specific table.
        
        Uses PostgreSQL information_schema to get accurate trigger information.
        """
        query = text("""
            SELECT 
                trigger_name,
                event_manipulation,
                action_timing,
                event_object_schema,
                event_object_table
            FROM information_schema.triggers
            WHERE event_object_schema = :schema_name 
              AND event_object_table = :table_name
            ORDER BY trigger_name
        """)
        
        triggers = []
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, {
                    "schema_name": schema_name,
                    "table_name": table_name
                })
                
                for row in result:
                    triggers.append(TriggerInfo(
                        trigger_name=row.trigger_name,
                        table_name=row.event_object_table,
                        schema_name=row.event_object_schema,
                        event_manipulation=row.event_manipulation,
                        action_timing=row.action_timing,
                        exists=True
                    ))
                    
        except Exception as e:
            logger.error(f"Failed to query triggers for {schema_name}.{table_name}: {e}")
        
        return triggers
    
    def get_trigger_functions(self, function_names: List[str]) -> List[FunctionInfo]:
        """
        Check if trigger functions exist and are valid.
        
        Queries PostgreSQL information_schema.routines for function information.
        """
        if not function_names:
            return []
        
        # Create parameter placeholders for the IN clause
        placeholders = ','.join([f':func_{i}' for i in range(len(function_names))])
        
        query = text(f"""
            SELECT 
                routine_name,
                routine_schema,
                routine_type,
                data_type
            FROM information_schema.routines
            WHERE routine_name IN ({placeholders})
              AND routine_type = 'FUNCTION'
            ORDER BY routine_name
        """)
        
        # Build parameters dictionary
        params = {f'func_{i}': func_name for i, func_name in enumerate(function_names)}
        
        found_functions = set()
        functions = []
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(query, params)
                
                for row in result:
                    found_functions.add(row.routine_name)
                    functions.append(FunctionInfo(
                        function_name=row.routine_name,
                        schema_name=row.routine_schema,
                        exists=True,
                        is_valid=True  # If it's in system catalog, it compiled successfully
                    ))
        
        except Exception as e:
            logger.error(f"Failed to query trigger functions {function_names}: {e}")
        
        # Add missing functions
        for func_name in function_names:
            if func_name not in found_functions:
                functions.append(FunctionInfo(
                    function_name=func_name,
                    schema_name="unknown",
                    exists=False,
                    is_valid=False
                ))
        
        return functions
    
    def get_expected_triggers(self, table_name: str) -> List[str]:
        """Get the list of trigger names we expect for a temporal table"""
        return [
            f"trg_{table_name}_insert",
            f"trg_{table_name}_update", 
            f"trg_{table_name}_delete"
        ]
    
    def get_expected_functions(self, table_name: str) -> List[str]:
        """Get the list of function names we expect for a temporal table"""
        return [
            f"trg_{table_name}_insert",
            f"trg_{table_name}_update",
            f"trg_{table_name}_delete",
            "calculate_systime_range"  # Shared utility function
        ]
    
    @contextmanager
    def test_transaction(self):
        """Context manager for test transactions that always rollback"""
        with self.engine.connect() as conn:
            trans = conn.begin()
            try:
                yield conn
            finally:
                trans.rollback()  # Always rollback test changes
    
    def test_triggers_functionally(self, config: TriggerConfig) -> Tuple[bool, Optional[str]]:
        """
        Functionally test triggers by performing INSERT/UPDATE/DELETE operations
        and verifying that history records are created.
        
        Uses transactions that are rolled back to avoid polluting data.
        """
        try:
            with self.test_transaction() as conn:
                # Generate test data
                test_id = str(uuid.uuid4())
                test_timestamp = datetime.now(UTC)
                
                # Build INSERT statement for primary table
                primary_columns = [config.key_column] + config.additional_columns
                
                # Create a simple test record - this is schema-agnostic
                if config.key_column.endswith('_id'):
                    # UUID primary key
                    test_key_value = f"'{test_id}'"
                else:
                    # String primary key (like 'cik', 'ticker')
                    test_key_value = f"'TEST_{test_id[:8]}'"
                
                # Build test values for additional columns (simplified)
                test_values = [test_key_value]
                for col in config.additional_columns:
                    if 'name' in col.lower():
                        test_values.append(f"'Test Name {test_id[:8]}'")
                    elif 'date' in col.lower() or 'time' in col.lower():
                        test_values.append(f"'{test_timestamp}'")
                    elif col.lower() in ['quantity', 'count', 'number']:
                        test_values.append("100")
                    elif col.lower() in ['price', 'amount', 'value']:
                        test_values.append("99.99")
                    else:
                        test_values.append(f"'TestValue_{test_id[:8]}'")
                
                schema_prefix = f"{config.schema_name}." if config.schema_name else ""
                
                # Test 1: INSERT trigger
                insert_sql = text(f"""
                    INSERT INTO {schema_prefix}{config.table_name} 
                    ({', '.join(primary_columns)})
                    VALUES ({', '.join(test_values)})
                """)
                
                conn.execute(insert_sql)
                
                # Verify history record was created
                history_check_sql = text(f"""
                    SELECT COUNT(*) as count 
                    FROM {schema_prefix}{config.history_table_name}
                    WHERE {config.key_column} = {test_key_value}
                      AND operation_type = true
                """)
                
                result = conn.execute(history_check_sql)
                insert_count = result.scalar()
                
                if insert_count != 1:
                    return False, f"INSERT trigger failed: expected 1 history record, found {insert_count}"
                
                # Test 2: UPDATE trigger
                update_sql = text(f"""
                    UPDATE {schema_prefix}{config.table_name}
                    SET {config.additional_columns[0]} = 'UPDATED_VALUE'
                    WHERE {config.key_column} = {test_key_value}
                """)
                
                conn.execute(update_sql)
                
                # Verify update created new history record
                update_check_sql = text(f"""
                    SELECT COUNT(*) as count
                    FROM {schema_prefix}{config.history_table_name}
                    WHERE {config.key_column} = {test_key_value}
                """)
                
                result = conn.execute(update_check_sql)
                total_count = result.scalar()
                
                if total_count < 2:
                    return False, f"UPDATE trigger failed: expected at least 2 history records, found {total_count}"
                
                # Test 3: DELETE trigger (verify it doesn't crash)
                delete_sql = text(f"""
                    DELETE FROM {schema_prefix}{config.table_name}
                    WHERE {config.key_column} = {test_key_value}
                """)
                
                conn.execute(delete_sql)
                
                logger.debug(f"✅ Functional test passed for {config.table_name}")
                return True, None
                
        except Exception as e:
            error_msg = f"Functional test failed for {config.table_name}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def validate_temporal_table(self, pair: TemporalTablePair, config: TriggerConfig) -> TemporalTableValidation:
        """
        Comprehensively validate triggers for a single temporal table.
        
        Performs static, functional, and coverage validation.
        """
        table_name = config.table_name
        schema_name = config.schema_name or "public"
        
        logger.info(f"🔍 Validating triggers for {schema_name}.{table_name}")
        
        # Get expected triggers and functions
        expected_triggers = self.get_expected_triggers(table_name)
        expected_functions = self.get_expected_functions(table_name)
        
        # Query database for actual triggers
        actual_triggers = self.get_database_triggers(schema_name, table_name)
        actual_trigger_names = {trigger.trigger_name for trigger in actual_triggers}
        
        # Find missing triggers
        missing_triggers = [name for name in expected_triggers if name not in actual_trigger_names]
        
        # Check trigger functions
        trigger_functions = self.get_trigger_functions(expected_functions)
        missing_functions = [func.function_name for func in trigger_functions if not func.exists]
        
        # Functional testing
        functional_test_passed = False
        functional_test_error = None
        
        if not missing_triggers and not missing_functions:
            functional_test_passed, functional_test_error = self.test_triggers_functionally(config)
        else:
            functional_test_error = "Skipped functional test due to missing triggers/functions"
        
        # Collect issues
        issues = []
        if missing_triggers:
            issues.append(f"Missing triggers: {', '.join(missing_triggers)}")
        if missing_functions:
            issues.append(f"Missing functions: {', '.join(missing_functions)}")
        if not functional_test_passed and functional_test_error:
            issues.append(f"Functional test failed: {functional_test_error}")
        
        is_valid = len(issues) == 0
        
        return TemporalTableValidation(
            table_name=table_name,
            schema_name=schema_name,
            expected_triggers=expected_triggers,
            actual_triggers=actual_triggers,
            missing_triggers=missing_triggers,
            trigger_functions=trigger_functions,
            missing_functions=missing_functions,
            functional_test_passed=functional_test_passed,
            functional_test_error=functional_test_error,
            is_valid=is_valid,
            issues=issues
        )
    
    def validate_all_triggers(self, temporal_pairs: List[TemporalTablePair]) -> TriggerValidationReport:
        """
        Validate triggers for all temporal table pairs.
        
        Returns comprehensive validation report.
        """
        logger.info(f"🔍 Starting comprehensive trigger validation for {len(temporal_pairs)} temporal pairs")
        
        validations = []
        
        for pair in temporal_pairs:
            # We need to get the trigger config for this pair
            from src._base.temporal_discovery import analyze_temporal_table_pair, generate_trigger_config
            
            analysis = analyze_temporal_table_pair(pair)
            config = generate_trigger_config(analysis)
            
            validation = self.validate_temporal_table(pair, config)
            validations.append(validation)
        
        # Generate summary
        total_tables = len(validations)
        valid_tables = sum(1 for v in validations if v.is_valid)
        invalid_tables = total_tables - valid_tables
        all_valid = invalid_tables == 0
        
        summary_issues = []
        if not all_valid:
            summary_issues.append(f"{invalid_tables} out of {total_tables} temporal tables have trigger issues")
            
            # Collect specific issues
            for validation in validations:
                if not validation.is_valid:
                    for issue in validation.issues:
                        summary_issues.append(f"{validation.table_name}: {issue}")
        
        return TriggerValidationReport(
            table_validations=validations,
            total_tables=total_tables,
            valid_tables=valid_tables,
            invalid_tables=invalid_tables,
            all_valid=all_valid,
            summary_issues=summary_issues
        )
    
    def print_validation_report(self, report: TriggerValidationReport) -> None:
        """Print a comprehensive, human-readable validation report"""
        
        logger.info("=" * 80)
        logger.info("🔍 TEMPORAL TRIGGER VALIDATION REPORT")
        logger.info("=" * 80)
        
        # Overall summary
        if report.all_valid:
            logger.info(f"🎉 SUCCESS: All {report.total_tables} temporal tables have valid triggers!")
        else:
            logger.error(f"❌ ISSUES FOUND: {report.invalid_tables}/{report.total_tables} temporal tables have trigger problems")
        
        # Detailed results for each table
        logger.info(f"\n📋 DETAILED VALIDATION RESULTS:")
        logger.info("-" * 50)
        
        for validation in report.table_validations:
            status = "✅" if validation.is_valid else "❌"
            logger.info(f"{status} {validation.schema_name}.{validation.table_name}")
            
            if validation.is_valid:
                logger.info(f"   • All {len(validation.expected_triggers)} triggers present and functional")
                logger.info(f"   • All {len(validation.trigger_functions)} functions exist")
                logger.info(f"   • Functional test: PASSED")
            else:
                for issue in validation.issues:
                    logger.info(f"   ⚠️  {issue}")
        
        # Summary of issues
        if not report.all_valid:
            logger.info(f"\n🚨 SUMMARY OF ISSUES:")
            logger.info("-" * 30)
            for issue in report.summary_issues:
                logger.info(f"• {issue}")
            
            logger.info(f"\n💡 RECOMMENDATION:")
            logger.info("   Review trigger creation logs and re-run build_triggers.py")
            logger.info("   Check database permissions and PostgreSQL function syntax")
        
        logger.info("=" * 80)


def validate_triggers_for_database(engine: Engine, temporal_pairs: List[TemporalTablePair]) -> bool:
    """
    Convenience function to validate triggers for a database.
    
    Returns True if all triggers are valid, False otherwise.
    """
    validator = TriggerValidator(engine)
    report = validator.validate_all_triggers(temporal_pairs)
    validator.print_validation_report(report)
    return report.all_valid