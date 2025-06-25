from datetime import datetime
from typing import Optional

from sqlalchemy import TIMESTAMP, Date, Column, text
from sqlmodel import Field, SQLModel


class FactRental(SQLModel, table=True):
    """
    Rental fact table - simple and focused.
    
    Facts rarely change after creation, so no temporal versioning needed.
    Partitioned by rental_date for performance.
    
    Example queries:
        -- Monthly rental volume
        SELECT DATE_TRUNC('month', rental_date) as month, COUNT(*)
        FROM fact_rental
        GROUP BY 1 ORDER BY 1;
        
        -- Average rental duration by month
        SELECT 
            DATE_TRUNC('month', rental_date) as month,
            AVG(EXTRACT(days FROM (return_date - rental_date))) as avg_days
        FROM fact_rental
        WHERE return_date IS NOT NULL
        GROUP BY 1 ORDER BY 1;
    """
    __tablename__ = "fact_rental"
    __table_args__ = {
        "schema": "temporal_silver",
        # Partition by month for performance
        "postgresql_partition_by": "RANGE (rental_date)"
    }

    # Composite primary key (required for partitioning)
    rental_id: int = Field(primary_key=True)
    rental_date: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False, primary_key=True))
    
    # Business keys
    customer_id: int = Field(nullable=False)
    film_id: int = Field(nullable=False)
    inventory_id: int = Field(nullable=False)
    staff_id: int = Field(nullable=False)
    store_id: int = Field(nullable=False)
    
    # Time dimensions
    return_date: Optional[datetime] = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=True))
    
    # Point-in-time snapshot column (for temporal model demonstration)
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