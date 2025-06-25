"""
Table type mixins and base classes for consistent schema patterns.

This module provides mixins and base classes to clearly categorize and standardize
different types of tables in our schema:
- Reference tables (lookup data with soft deletes)
- Transactional tables (business entities with audit trails)  
- Temporal tables (history tracking with system versioning)
"""

import uuid
from datetime import UTC, datetime
from typing import Any, Dict, Optional

from sqlalchemy import SMALLINT, TIMESTAMP, Column, text
from sqlmodel import Field, SQLModel


class ReferenceTableMixin:
    """
    Mixin for reference/lookup tables.
    
    Adds standard fields for reference data:
    - inactivated_date: UTC datetime when record was inactivated (NULL = active)
    - systime: audit timestamp for when record was created/last modified
    
    Reference tables should use SMALLINT primary keys for performance.
    
    Active vs Inactive Logic:
    - inactivated_date IS NULL → Record is ACTIVE
    - inactivated_date IS NOT NULL → Record was INACTIVATED on that date
    """
    
    inactivated_date: Optional[datetime] = Field(
        default=None,
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True)
    )
    systime: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )


class TransactionalTableMixin:
    """
    Mixin for transactional/business entity tables.
    
    Adds standard audit fields:
    - effective_time: business effective timestamp
    - systime: system audit timestamp (optional, can be overridden)
    
    Transactional tables typically use UUID primary keys.
    """
    
    effective_time: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        sa_column=Column(TIMESTAMP(timezone=True), nullable=False)
    )


class TemporalTableMixin:
    """
    Mixin for temporal/history tables.
    
    Adds standard temporal fields for system versioning:
    - sys_start: system-time start (when record became current)
    - sys_end: system-time end (when record was superseded, 'infinity' for current)
    
    Used with PostgreSQL temporal extensions or manual history tracking.
    """
    
    sys_start: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            primary_key=True,
            default=text("clock_timestamp()")
        )
    )
    sys_end: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            default=text("'infinity'")
        )
    )


class ReferenceTableBase(ReferenceTableMixin, SQLModel, table=True):
    """
    Base class for reference/lookup tables.
    
    Provides:
    - SMALLINT primary key (override field name in subclass)
    - inactivated_date for temporal soft deletes (NULL = active)
    - systime for audit trail
    
    """
    
    # This is abstract - subclasses must define their own primary key
    __abstract__ = True


class TransactionalTableBase(TransactionalTableMixin, SQLModel, table=True):
    """
    Base class for transactional/business entity tables.
    
    Provides:
    - UUID primary key (override field name in subclass)
    - effective_time for business temporal tracking
    
    """
    
    # This is abstract - subclasses must define their own primary key
    __abstract__ = True


class TemporalTableBase(TemporalTableMixin, SQLModel, table=True):
    """
    Base class for temporal/history tables.
    
    Provides:
    - sys_start/sys_end for system versioning
    - Composite primary key with business key + sys_start
    
    """
    
    # This is abstract - subclasses must define their own business primary key
    __abstract__ = True


def get_table_type(table_class: type) -> str:
    """
    Classify a table by its inheritance hierarchy and naming conventions.
    
    Returns:
        str: One of "reference", "transactional", "temporal", "unknown"
    """
    
    # Check inheritance hierarchy first (most reliable)
    if issubclass(table_class, ReferenceTableBase):
        return "reference"
    elif issubclass(table_class, TransactionalTableBase):
        return "transactional"
    elif issubclass(table_class, TemporalTableBase):
        return "temporal"
    
    # Fallback to mixin detection
    if issubclass(table_class, ReferenceTableMixin):
        return "reference"
    elif issubclass(table_class, TransactionalTableMixin):
        return "transactional"
    elif issubclass(table_class, TemporalTableMixin):
        return "temporal"
    
    # Fallback to naming conventions
    table_name = getattr(table_class, "__tablename__", "")
    if table_name.endswith("_type") or table_name.endswith("_status_type"):
        return "reference"
    elif table_name.endswith("_hist"):
        return "temporal"
    
    return "unknown"


def validate_table_conventions(table_class: type) -> Dict[str, Any]:
    """
    Validate that a table follows conventions for its type.
    
    Returns:
        Dict with validation results and recommendations.
    """
    
    table_type = get_table_type(table_class)
    table_name = getattr(table_class, "__tablename__", "unknown")
    issues = []
    recommendations = []
    
    if table_type == "reference":
        # Check for inactivated_date field
        if not hasattr(table_class, "inactivated_date"):
            issues.append("Reference table missing 'inactivated_date' field")
            recommendations.append("Add 'inactivated_date: Optional[datetime] = Field(default=None)' or inherit from ReferenceTableBase")
        
        # Check naming convention
        if not (table_name.endswith("_type") or table_name.endswith("_status_type")):
            recommendations.append(f"Consider renaming '{table_name}' to follow '*_type' convention")
    
    elif table_type == "transactional":
        # Check for effective_time field
        if not hasattr(table_class, "effective_time"):
            recommendations.append("Consider adding 'effective_time' field for business temporal tracking")
    
    elif table_type == "temporal":
        # Check for temporal fields
        if not (hasattr(table_class, "sys_start") and hasattr(table_class, "sys_end")):
            issues.append("Temporal table missing 'sys_start' and/or 'sys_end' fields")
    
    return {
        "table_name": table_name,
        "table_type": table_type,
        "issues": issues,
        "recommendations": recommendations,
        "valid": len(issues) == 0
    }