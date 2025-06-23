from datetime import datetime
from typing import Any, Dict

from sqlalchemy import BIGINT, CHAR, TIMESTAMP, Column, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class BrCustomerCdc(SQLModel, table=True):
    """
    CDC bronze table for customer changes.
    
    Captures raw change data from PostgreSQL logical replication (wal2json/pgoutput).
    Each row represents a single change operation with full payload preservation.
    
    Time-travel example:
        SELECT full_row->>'first_name' as name, load_ts
        FROM br_customer_cdc 
        WHERE full_row->>'customer_id' = '42'
        ORDER BY lsn;
    """
    __tablename__ = "br_customer_cdc"
    __table_args__ = {"schema": "cdc_bronze"}

    # WAL Log Sequence Number - provides ordering guarantee
    lsn: int = Field(sa_column=Column(BIGINT, primary_key=True))
    
    # Operation type: I(nsert), U(pdate), D(elete)
    op: str = Field(sa_column=Column(CHAR(1), nullable=False))
    
    # Complete row payload as JSON - handles schema evolution gracefully
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