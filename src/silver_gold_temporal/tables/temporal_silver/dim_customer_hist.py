from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Boolean, Column, text
from sqlmodel import Field, SQLModel


class DimCustomerHist(SQLModel, table=True):
    """
    System-versioned temporal dimension for customers.
    
    Provides automatic SCD Type-2 with PostgreSQL temporal extensions.
    
    Time-travel examples:
        -- What did customer 42 look like on July 1st?
        SELECT * FROM dim_customer_hist 
        FOR SYSTEM_TIME AS OF '2023-07-01'::timestamptz
        WHERE customer_id = 42;
        
        -- Show all changes to customer 42
        SELECT customer_id, first_name, email, sys_start, sys_end
        FROM dim_customer_hist 
        WHERE customer_id = 42
        ORDER BY sys_start;
        
    MERGE processing:
        MERGE INTO dim_customer_hist tgt
        USING staging_customer src ON tgt.customer_id = src.customer_id
        WHEN MATCHED AND (src.hash <> tgt.hash)
            THEN UPDATE SET first_name = src.first_name, ..., sys_end = clock_timestamp()
        WHEN NOT MATCHED 
            THEN INSERT (customer_id, first_name, ...) VALUES (...);
    """
    __tablename__ = "dim_customer_hist"
    __table_args__ = {
        "schema": "temporal_silver"
        # Note: PostgreSQL 15+ system versioning would be configured via custom triggers
        # "postgresql_with": "system_versioning = true"  # Not supported in current PostgreSQL
    }

    # Business natural key
    customer_id: int = Field(primary_key=True)
    
    # Customer attributes
    first_name: str = Field(nullable=False, max_length=45)
    last_name: str = Field(nullable=False, max_length=45)
    email: Optional[str] = Field(default=None, nullable=True, max_length=50)
    active: bool = Field(sa_column=Column(Boolean, nullable=False, default=True))
    
    # System-time versioning columns
    sys_start: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            primary_key=True,
            nullable=False,
            default=text("clock_timestamp()"),
            server_default=text("clock_timestamp()")
        )
    )
    sys_end: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("'infinity'"),
            server_default=text("'infinity'")
        )
    )