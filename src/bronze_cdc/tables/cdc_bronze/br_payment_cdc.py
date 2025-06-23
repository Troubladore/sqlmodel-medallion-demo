from datetime import datetime
from typing import Any, Dict

from sqlalchemy import BIGINT, CHAR, TIMESTAMP, Column, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class BrPaymentCdc(SQLModel, table=True):
    """
    CDC bronze table for payment transactions.
    
    Note: Original Pagila already partitions payments by month, but CDC 
    captures changes regardless of target partitioning strategy.
    This demonstrates that CDC ≠ partition strategy.
    
    Revenue analysis example:
        SELECT 
            DATE_TRUNC('month', (full_row->>'payment_date')::timestamptz) as month,
            SUM((full_row->>'amount')::numeric) as revenue
        FROM br_payment_cdc 
        WHERE op = 'I'  -- only count inserts
        GROUP BY 1 ORDER BY 1;
    """
    __tablename__ = "br_payment_cdc"
    __table_args__ = {"schema": "cdc_bronze"}

    # WAL Log Sequence Number - provides ordering guarantee
    lsn: int = Field(sa_column=Column(BIGINT, primary_key=True))
    
    # Operation type: I(nsert), U(pdate), D(elete)
    op: str = Field(sa_column=Column(CHAR(1), nullable=False))
    
    # Complete row payload as JSON - payment amounts and dates
    full_row: Dict[str, Any] = Field(sa_column=Column(JSONB, nullable=False))
    
    # When this change was captured and loaded
    load_ts: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )