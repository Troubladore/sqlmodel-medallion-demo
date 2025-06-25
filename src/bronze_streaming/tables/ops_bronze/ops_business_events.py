from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID, uuid4
from enum import Enum
from sqlalchemy import TIMESTAMP, Column, text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class BusinessEventType(str, Enum):
    """Types of business events captured in real-time."""
    RENTAL_START = "rental_start"
    RENTAL_RETURN = "rental_return" 
    PAYMENT_PROCESSED = "payment_processed"
    INVENTORY_LOW = "inventory_low"
    CUSTOMER_SIGNUP = "customer_signup"
    CUSTOMER_UPDATE = "customer_update"


class OpsBrozeBusinessEvents(SQLModel, table=True):
    """
    Real-time business events from the Pagila video rental system.
    
    Captures operational events like rentals, returns, payments as they happen.
    Designed for streaming ingestion via PostgreSQL LISTEN/NOTIFY.
    """
    __tablename__ = "ops_business_events"
    __table_args__ = {"schema": "ops_bronze"}
    
    # Primary key and timing
    event_id: UUID = Field(default_factory=uuid4, primary_key=True)
    event_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), 
                        default=text("clock_timestamp()"),
                        nullable=False)
    )
    
    # Event classification
    event_type: BusinessEventType = Field(
        sa_column=Column(SQLEnum(BusinessEventType), nullable=False)
    )
    source_table: str = Field(max_length=50, nullable=False)
    source_id: Optional[str] = Field(max_length=100, default=None)
    
    # Event payload (flexible JSON structure)
    event_data: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False),
        description="Complete event payload with all relevant data"
    )
    
    # Operational metadata
    store_id: Optional[int] = Field(default=None, description="Store where event occurred")
    customer_id: Optional[int] = Field(default=None, description="Customer involved in event")
    staff_id: Optional[int] = Field(default=None, description="Staff member who triggered event")
    
    # System metadata
    processing_status: str = Field(default="pending", max_length=20)
    processed_at: Optional[datetime] = Field(default=None)
    retry_count: int = Field(default=0)
    
    # Partitioning hint for performance
    event_date: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("date_trunc('day', clock_timestamp())"),
                        nullable=False),
        description="Date for partitioning (truncated to day)"
    )