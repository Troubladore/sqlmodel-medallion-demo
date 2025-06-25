from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, Column
from sqlmodel import Field, SQLModel


class DimCustomer(SQLModel, table=False):
    """
    Current-state view of customer dimension.
    
    Provides a "normal" table interface that hides temporal complexity.
    Only shows current rows (where sys_end = 'infinity').
    
    SQL Definition:
        CREATE OR REPLACE VIEW temporal_silver.dim_customer AS
        SELECT customer_id, first_name, last_name, email, active, sys_start
        FROM temporal_silver.dim_customer_hist
        WHERE sys_end = 'infinity'::timestamptz;
    
    Usage:
        -- Use like a normal table
        SELECT * FROM dim_customer WHERE customer_id = 42;
        
        -- Time-travel when needed
        SELECT * FROM dim_customer_hist 
        FOR SYSTEM_TIME AS OF '2023-07-01' 
        WHERE customer_id = 42;
    """
    __tablename__ = "dim_customer"
    __table_args__ = {"schema": "temporal_silver"}

    # Business natural key
    customer_id: int = Field(primary_key=True)
    
    # Customer attributes
    first_name: str = Field(nullable=False, max_length=45)
    last_name: str = Field(nullable=False, max_length=45)
    email: Optional[str] = Field(default=None, nullable=True, max_length=50)
    active: bool = Field(sa_column=Column(Boolean, nullable=False, default=True))
    
    # Current version timestamp (for reference)
    sys_start: datetime = Field(nullable=False)