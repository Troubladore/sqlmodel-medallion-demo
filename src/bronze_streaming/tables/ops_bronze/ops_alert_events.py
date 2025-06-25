from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID, uuid4
from enum import Enum
from sqlalchemy import TIMESTAMP, Column, text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class AlertSeverity(str, Enum):
    """Alert severity levels for operational monitoring."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    RESOLVED = "resolved"


class AlertCategory(str, Enum):
    """Categories of operational alerts."""
    SYSTEM_PERFORMANCE = "system_performance"
    BUSINESS_THRESHOLD = "business_threshold"
    DATA_QUALITY = "data_quality"
    INVENTORY_ALERT = "inventory_alert"
    CUSTOMER_ISSUE = "customer_issue"
    SECURITY_EVENT = "security_event"


class OpsAlertEvents(SQLModel, table=True):
    """
    Real-time operational alerts and threshold breaches.
    
    Captures system alerts, business rule violations, and anomalies
    for immediate operational response.
    """
    __tablename__ = "ops_alert_events"
    __table_args__ = {"schema": "ops_bronze"}
    
    # Primary key and timing
    alert_id: UUID = Field(default_factory=uuid4, primary_key=True)
    alert_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), 
                        default=text("clock_timestamp()"),
                        nullable=False)
    )
    
    # Alert classification
    alert_category: AlertCategory = Field(
        sa_column=Column(SQLEnum(AlertCategory), nullable=False)
    )
    severity: AlertSeverity = Field(
        sa_column=Column(SQLEnum(AlertSeverity), nullable=False)
    )
    alert_name: str = Field(max_length=100, nullable=False)
    alert_description: str = Field(max_length=500, nullable=False)
    
    # Source information
    source_metric: Optional[str] = Field(max_length=100, default=None)
    source_table: Optional[str] = Field(max_length=50, default=None)
    source_value: Optional[float] = Field(default=None)
    threshold_value: Optional[float] = Field(default=None)
    
    # Context and payload
    alert_context: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False),
        description="Full context data for alert investigation"
    )
    
    # Operational dimensions
    store_id: Optional[int] = Field(default=None)
    customer_id: Optional[int] = Field(default=None)
    affected_system: str = Field(max_length=50, default="pagila")
    
    # Alert lifecycle
    is_active: bool = Field(default=True)
    acknowledged_at: Optional[datetime] = Field(default=None)
    acknowledged_by: Optional[str] = Field(max_length=50, default=None)
    resolved_at: Optional[datetime] = Field(default=None)
    resolution_notes: Optional[str] = Field(max_length=1000, default=None)
    
    # Escalation tracking
    escalation_level: int = Field(default=0)
    last_escalated_at: Optional[datetime] = Field(default=None)
    notification_sent: bool = Field(default=False)
    
    # Performance partitioning
    alert_date: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("date_trunc('day', clock_timestamp())"),
                        nullable=False),
        description="Date for partitioning"
    )