from datetime import datetime

from sqlalchemy import TIMESTAMP, Numeric, Column, text
from sqlmodel import Field, SQLModel


class FactPayment(SQLModel, table=True):
    """
    Payment fact table - simple and focused.
    
    Inherits partitioning strategy from original Pagila (by payment_date).
    Facts are insert-only, no temporal versioning needed.
    
    Example queries:
        -- Monthly revenue by store
        SELECT 
            store_id,
            DATE_TRUNC('month', payment_date) as month,
            SUM(amount) as revenue
        FROM fact_payment
        GROUP BY 1, 2 ORDER BY 1, 2;
        
        -- Average payment amount over time
        SELECT 
            DATE_TRUNC('month', payment_date) as month,
            AVG(amount) as avg_payment
        FROM fact_payment
        GROUP BY 1 ORDER BY 1;
    """
    __tablename__ = "fact_payment"
    __table_args__ = {
        "schema": "temporal_silver",
        # Inherits Pagila's monthly partitioning strategy
        "postgresql_partition_by": "RANGE (payment_date)"
    }

    # Composite primary key (required for partitioning)
    payment_id: int = Field(primary_key=True)
    payment_date: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False, primary_key=True))
    
    # Business keys
    customer_id: int = Field(nullable=False)
    staff_id: int = Field(nullable=False)
    rental_id: int = Field(nullable=False)
    store_id: int = Field(nullable=False)  # Denormalized for performance
    
    # Measures
    amount: float = Field(sa_column=Column(Numeric(5, 2), nullable=False))
    
    # Point-in-time snapshot column
    snapshot_date: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("CURRENT_DATE"),
            server_default=text("CURRENT_DATE")
        )
    )
    
    # ETL audit
    load_ts: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )