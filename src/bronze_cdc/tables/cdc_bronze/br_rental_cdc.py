from datetime import datetime
from typing import Any, Dict

from sqlalchemy import BIGINT, CHAR, TIMESTAMP, Column, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class BrRentalCdc(SQLModel, table=True):
    """
    CDC bronze table for rental transactions.
    
    High-volume fact table perfect for demonstrating:
    - Streaming CDC ingestion patterns
    - Watermark-based processing 
    - Real-time analytics capabilities
    
    Watermark example:
        SELECT MAX(lsn) as high_water_mark 
        FROM br_rental_cdc 
        WHERE load_ts < now() - interval '5 minutes';
    """
    __tablename__ = "br_rental_cdc"
    __table_args__ = {"schema": "cdc_bronze"}

    # WAL Log Sequence Number - provides ordering guarantee
    lsn: int = Field(sa_column=Column(BIGINT, primary_key=True))
    
    # Operation type: I(nsert), U(pdate), D(elete)
    op: str = Field(sa_column=Column(CHAR(1), nullable=False))
    
    # Complete row payload as JSON - rental transactions with timestamps
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