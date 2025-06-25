"""
Temporal Table Auto-Discovery System with Selective Field Exclusion

This module provides elegant auto-discovery and validation of temporal table pairs,
with sophisticated support for excluding specific fields from history tracking.

🎯 Key Features:
- Auto-discover primary table → __history table pairs using naming conventions
- Validate that history tables contain the correct fields (respecting exclusions)
- Generate trigger configurations automatically with excluded fields filtered out
- Support field-level and class-level exclusion annotations
- Comprehensive validation reporting with exclusion analysis

🔧 Field Exclusion API:

Method 1 - Field-level annotations (Recommended):
```python
class Trade(SQLModel, table=True):
    trade_id: UUID = Field(primary_key=True)
    quantity: int = Field()                                    # ✅ Tracked in history
    
    # Exclude specific fields from history tracking
    batch_id: str = Field(temporal_exclude=True)               # 🚫 Not tracked
    large_document: bytes = Field(temporal_exclude=True)       # 🚫 Not tracked  
    computed_cache: str = Field(temporal_exclude=True)         # 🚫 Not tracked
```

Method 2 - Class-level exclusions:
```python
class Document(SQLModel, table=True):
    __temporal_exclude__ = ['content_blob', 'search_cache']    # 🚫 Not tracked
    
    document_id: UUID = Field(primary_key=True)               # ✅ Tracked in history
    title: str = Field()                                       # ✅ Tracked in history
    content_blob: bytes = Field()                              # 🚫 Not tracked (excluded)
    search_cache: str = Field()                                # 🚫 Not tracked (excluded)
```

🧪 Built-in Exclusions:
- effective_time: Always excluded (handled specially by trigger logic)

📋 Validation Rules:
1. History table must contain all TRACKABLE primary table fields
2. History table must contain required temporal fields (history_id, effective_time, systime, operation_type)  
3. History table must NOT contain excluded fields
4. Primary key field must be auto-detectable

🎉 Example temporal pair validation:
- Primary: demo.trade (with some fields marked temporal_exclude=True)
- History: demo.trade__history (containing only trackable fields + temporal fields)

See examples/temporal_field_exclusion_example.py for comprehensive usage examples.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Any, Type, Tuple
from sqlmodel import SQLModel
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from src._base.tables.history_table import HistoryTable
from utils.utils import discover_tables_in_database, extract_table_classes_from_modules

logger = logging.getLogger(__name__)


@dataclass
class TemporalTablePair:
    """Represents a primary table and its corresponding history table"""
    primary_table: Type[SQLModel]
    history_table: Type[SQLModel] 
    base_name: str
    
    @property
    def primary_table_name(self) -> str:
        return self.primary_table.__tablename__
    
    @property
    def history_table_name(self) -> str:
        return self.history_table.__tablename__
    
    @property
    def schema_name(self) -> Optional[str]:
        """Extract schema from table_args"""
        if hasattr(self.primary_table, '__table_args__'):
            table_args = self.primary_table.__table_args__
            if isinstance(table_args, dict):
                return table_args.get('schema')
            elif isinstance(table_args, tuple) and len(table_args) > 0:
                if isinstance(table_args[-1], dict):
                    return table_args[-1].get('schema')
        return None


@dataclass
class TemporalTableAnalysis:
    """Analysis results for a temporal table pair"""
    pair: TemporalTablePair
    primary_fields: Dict[str, Any]
    history_fields: Dict[str, Any] 
    trigger_columns: List[str]
    primary_key_column: str
    missing_fields: Set[str]
    extra_fields: Set[str]
    is_valid: bool
    validation_messages: List[str]


@dataclass
class TriggerConfig:
    """Configuration for trigger generation"""
    schema_name: Optional[str]
    table_name: str
    history_table_name: str
    key_column: str
    additional_columns: List[str]


def get_table_fields(table_class: Type[SQLModel]) -> Dict[str, Any]:
    """Extract field definitions from a SQLModel table class"""
    fields = {}
    
    # Get fields from SQLModel annotations
    if hasattr(table_class, '__annotations__'):
        for field_name, field_type in table_class.__annotations__.items():
            if not field_name.startswith('_'):  # Skip private attributes
                fields[field_name] = field_type
    
    return fields


def get_field_exclusions(table_class: Type[SQLModel]) -> Set[str]:
    """
    Extract fields that should be excluded from history tracking.
    
    Fields can be excluded via:
    1. temporal_exclude=True in Field() definition
    2. Built-in exclusions (effective_time - handled specially in triggers)
    
    Example:
        batch_id: str = Field(temporal_exclude=True)
        large_blob: bytes = Field(temporal_exclude=True)
    """
    excluded_fields = set()
    
    # Built-in exclusions - fields handled specially by trigger logic
    built_in_exclusions = {'effective_time'}
    excluded_fields.update(built_in_exclusions)
    
    # Check for temporal_exclude annotations in Field() definitions
    if hasattr(table_class, '__fields__'):
        for field_name, field_info in table_class.__fields__.items():
            if hasattr(field_info, 'field_info') and field_info.field_info:
                # Check if field is marked for temporal exclusion
                extra = getattr(field_info.field_info, 'extra', {})
                if extra.get('temporal_exclude', False):
                    excluded_fields.add(field_name)
                    logger.debug(f"Field {field_name} excluded from history via temporal_exclude=True")
    
    # Fallback: check class-level __temporal_exclude__ if present (alternative API)
    if hasattr(table_class, '__temporal_exclude__'):
        class_exclusions = table_class.__temporal_exclude__
        if isinstance(class_exclusions, (list, tuple, set)):
            excluded_fields.update(class_exclusions)
            logger.debug(f"Fields {class_exclusions} excluded via __temporal_exclude__ class attribute")
    
    return excluded_fields


def get_trackable_fields(table_class: Type[SQLModel]) -> Dict[str, Any]:
    """
    Get fields that should be tracked in history tables.
    
    This is all fields EXCEPT those marked for exclusion.
    """
    all_fields = get_table_fields(table_class)
    excluded_fields = get_field_exclusions(table_class)
    
    trackable_fields = {
        name: field_type 
        for name, field_type in all_fields.items() 
        if name not in excluded_fields
    }
    
    logger.debug(f"Trackable fields for {table_class.__name__}: {list(trackable_fields.keys())}")
    logger.debug(f"Excluded fields for {table_class.__name__}: {list(excluded_fields)}")
    
    return trackable_fields


def get_primary_key_field(table_class: Type[SQLModel]) -> str:
    """Find the primary key field name for a table"""
    
    # Try to inspect the table's fields for primary key
    if hasattr(table_class, '__fields__'):
        for field_name, field_info in table_class.__fields__.items():
            if hasattr(field_info, 'field_info') and field_info.field_info:
                # Check if field is marked as primary key
                if getattr(field_info.field_info, 'primary_key', False):
                    return field_name
    
    # Fallback: try common primary key patterns
    fields = get_table_fields(table_class)
    table_name = table_class.__tablename__
    
    # Common patterns: table_id, id
    potential_keys = [f"{table_name}_id", "id"]
    for key in potential_keys:
        if key in fields:
            return key
    
    # If still not found, return the first field as fallback
    field_names = list(fields.keys())
    if field_names:
        logger.warning(f"Could not determine primary key for {table_name}, using first field: {field_names[0]}")
        return field_names[0]
    
    raise ValueError(f"Could not determine primary key for table {table_name}")


def discover_temporal_table_pairs(database_tables: List[Type[SQLModel]]) -> List[TemporalTablePair]:
    """
    Auto-discover temporal table pairs using naming conventions.
    
    Logic:
    - Find tables ending with '__history' that inherit from HistoryTable
    - Match them with primary tables having the base name
    - Return validated pairs
    """
    
    primary_tables = {}
    history_tables = {}
    
    # Separate tables by type
    for table_class in database_tables:
        if not hasattr(table_class, '__tablename__'):
            continue
            
        table_name = table_class.__tablename__
        
        if table_name.endswith("__history"):
            # This is a history table
            base_name = table_name[:-9]  # Remove "__history" suffix
            
            # Verify it inherits from HistoryTable
            if issubclass(table_class, HistoryTable):
                history_tables[base_name] = table_class
                logger.debug(f"Found history table: {table_name} (base: {base_name})")
            else:
                logger.warning(f"Table {table_name} has __history suffix but doesn't inherit from HistoryTable")
                
        elif not issubclass(table_class, HistoryTable):
            # This is a potential primary table (not a history table)
            primary_tables[table_name] = table_class
            logger.debug(f"Found primary table: {table_name}")
    
    # Find matching pairs
    pairs = []
    for base_name, history_table in history_tables.items():
        if base_name in primary_tables:
            pair = TemporalTablePair(
                primary_table=primary_tables[base_name],
                history_table=history_table,
                base_name=base_name
            )
            pairs.append(pair)
            logger.info(f"✅ Discovered temporal pair: {base_name} → {base_name}__history")
        else:
            logger.warning(f"⚠️  Orphaned history table: {base_name}__history (no matching primary table)")
    
    return pairs


def analyze_temporal_table_pair(pair: TemporalTablePair) -> TemporalTableAnalysis:
    """
    Analyze and validate a temporal table pair with support for field exclusions.
    
    Enhanced validation rules:
    1. History table must have all TRACKABLE primary table fields (respects temporal_exclude)
    2. History table must have temporal fields: history_id, effective_time, systime, operation_type
    3. History table must NOT contain excluded fields
    4. Primary key field must be identifiable
    """
    
    # Get all fields and trackable fields (excluding temporal_exclude=True fields)
    primary_all_fields = get_table_fields(pair.primary_table)
    primary_trackable_fields = get_trackable_fields(pair.primary_table)
    primary_excluded_fields = get_field_exclusions(pair.primary_table)
    history_fields = get_table_fields(pair.history_table)
    
    # Required temporal fields in history table
    required_temporal_fields = {'history_id', 'effective_time', 'systime', 'operation_type'}
    
    # Expected fields in history table = trackable primary fields + temporal fields
    expected_history_fields = set(primary_trackable_fields.keys()) | required_temporal_fields
    actual_history_fields = set(history_fields.keys())
    
    # Find discrepancies
    missing_fields = expected_history_fields - actual_history_fields
    extra_fields = actual_history_fields - expected_history_fields
    
    # Check for excluded fields incorrectly present in history table
    excluded_in_history = primary_excluded_fields & actual_history_fields
    
    # Generate validation messages
    validation_messages = []
    
    if missing_fields:
        validation_messages.append(f"Missing trackable fields in history table: {', '.join(sorted(missing_fields))}")
    
    if excluded_in_history:
        validation_messages.append(f"Excluded fields incorrectly present in history table: {', '.join(sorted(excluded_in_history))}")
        validation_messages.append("  Hint: These fields are marked temporal_exclude=True but exist in history table")
    
    if extra_fields:
        # Filter out the excluded fields from "extra" since we already reported them
        truly_extra = extra_fields - excluded_in_history
        if truly_extra:
            validation_messages.append(f"Unexpected extra fields in history table: {', '.join(sorted(truly_extra))}")
    
    # Report exclusions for transparency
    if primary_excluded_fields:
        logger.info(f"📋 Fields excluded from history tracking for {pair.base_name}: {', '.join(sorted(primary_excluded_fields))}")
    
    # Determine primary key
    try:
        primary_key_column = get_primary_key_field(pair.primary_table)
    except ValueError as e:
        validation_messages.append(str(e))
        primary_key_column = "unknown"
    
    # Determine trigger columns (trackable fields only, since excluded fields shouldn't be in triggers)
    trigger_columns = list(primary_trackable_fields.keys())
    
    # Validation passes if no missing trackable fields, no excluded fields in history, and PK identified
    is_valid = (len(missing_fields) == 0 and 
                len(excluded_in_history) == 0 and 
                primary_key_column != "unknown")
    
    return TemporalTableAnalysis(
        pair=pair,
        primary_fields=primary_all_fields,  # Keep for backward compatibility
        history_fields=history_fields,
        trigger_columns=trigger_columns,
        primary_key_column=primary_key_column,
        missing_fields=missing_fields,
        extra_fields=extra_fields,
        is_valid=is_valid,
        validation_messages=validation_messages
    )


def generate_trigger_config(analysis: TemporalTableAnalysis) -> TriggerConfig:
    """Generate trigger configuration from table analysis"""
    
    return TriggerConfig(
        schema_name=analysis.pair.schema_name,
        table_name=analysis.pair.primary_table_name,
        history_table_name=analysis.pair.history_table_name,
        key_column=analysis.primary_key_column,
        additional_columns=analysis.trigger_columns
    )


def discover_temporal_tables_for_database(src_path: str, database_name: str) -> List[TemporalTablePair]:
    """
    Discover all temporal table pairs in a database.
    
    Returns:
        List of validated temporal table pairs ready for trigger generation
    """
    
    logger.info(f"🔍 Discovering temporal tables for database: {database_name}")
    
    # Discover all tables in the database
    table_modules = discover_tables_in_database(src_path, database_name)
    if not table_modules:
        logger.info(f"No table modules found for database {database_name}")
        return []
    
    # Extract table classes from modules
    all_tables = extract_table_classes_from_modules(table_modules)
    if not all_tables:
        logger.info(f"No table classes found for database {database_name}")
        return []
    
    logger.info(f"Found {len(all_tables)} total table classes")
    
    # Discover temporal pairs
    temporal_pairs = discover_temporal_table_pairs(all_tables)
    
    if not temporal_pairs:
        logger.info(f"No temporal table pairs found in database {database_name}")
        return []
    
    logger.info(f"🎯 Found {len(temporal_pairs)} temporal table pairs")
    
    # Validate each pair and filter to only valid ones
    valid_pairs = []
    for pair in temporal_pairs:
        analysis = analyze_temporal_table_pair(pair)
        
        if analysis.is_valid:
            logger.info(f"✅ Validated temporal pair: {pair.base_name}")
            valid_pairs.append(pair)
        else:
            logger.error(f"❌ Invalid temporal pair: {pair.base_name}")
            for message in analysis.validation_messages:
                logger.error(f"   {message}")
    
    logger.info(f"🎉 {len(valid_pairs)} valid temporal pairs ready for trigger generation")
    return valid_pairs


def print_temporal_discovery_summary(src_path: str, database_name: str) -> None:
    """Print a comprehensive summary of temporal table discovery for debugging"""
    
    logger.info("=" * 80)
    logger.info(f"🔍 TEMPORAL TABLE DISCOVERY SUMMARY - {database_name}")
    logger.info("=" * 80)
    
    # Discover all tables
    table_modules = discover_tables_in_database(src_path, database_name)
    all_tables = extract_table_classes_from_modules(table_modules) if table_modules else []
    
    # Categorize tables
    primary_tables = []
    history_tables = []
    other_tables = []
    
    for table_class in all_tables:
        if not hasattr(table_class, '__tablename__'):
            continue
            
        table_name = table_class.__tablename__
        
        if table_name.endswith("__history"):
            history_tables.append((table_name, issubclass(table_class, HistoryTable)))
        elif issubclass(table_class, HistoryTable):
            other_tables.append((table_name, "abstract_history"))
        else:
            primary_tables.append(table_name)
    
    logger.info(f"📊 TABLE INVENTORY:")
    logger.info(f"   Primary tables: {len(primary_tables)}")
    logger.info(f"   History tables: {len(history_tables)}")
    logger.info(f"   Other tables: {len(other_tables)}")
    
    if primary_tables:
        logger.info(f"📋 PRIMARY TABLES:")
        for table in sorted(primary_tables):
            logger.info(f"   • {table}")
    
    if history_tables:
        logger.info(f"📚 HISTORY TABLES:")
        for table_name, inherits_correctly in sorted(history_tables):
            status = "✅" if inherits_correctly else "❌"
            logger.info(f"   {status} {table_name}")
    
    # Discover and analyze pairs
    temporal_pairs = discover_temporal_table_pairs(all_tables)
    
    if temporal_pairs:
        logger.info(f"🔗 TEMPORAL PAIRS ANALYSIS:")
        for pair in temporal_pairs:
            analysis = analyze_temporal_table_pair(pair)
            status = "✅" if analysis.is_valid else "❌"
            logger.info(f"   {status} {pair.base_name} → {pair.base_name}__history")
            
            if not analysis.is_valid:
                for message in analysis.validation_messages:
                    logger.info(f"      ⚠️  {message}")
    
    logger.info("=" * 80)