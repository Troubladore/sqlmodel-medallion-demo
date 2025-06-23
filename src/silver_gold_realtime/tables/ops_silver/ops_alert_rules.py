from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID, uuid4
from enum import Enum
from sqlalchemy import TIMESTAMP, Boolean, Numeric, Column, text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class AlertCondition(str, Enum):
    """Types of alert conditions for monitoring."""
    GREATER_THAN = "greater_than"
    LESS_THAN = "less_than"
    EQUALS = "equals"
    NOT_EQUALS = "not_equals"
    PERCENTAGE_CHANGE = "percentage_change"
    ANOMALY_DETECTED = "anomaly_detected"
    MISSING_DATA = "missing_data"


class OpsAlertRules(SQLModel, table=True):
    """
    Configuration for automated alert rules and thresholds.
    
    Defines business rules and system thresholds for operational monitoring.
    Used by real-time processing to generate alerts automatically.
    """
    __tablename__ = "ops_alert_rules"
    __table_args__ = {"schema": "ops_silver"}
    
    # Primary key
    rule_id: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Rule identification
    rule_name: str = Field(max_length=100, nullable=False, unique=True)
    rule_description: str = Field(max_length=500, nullable=False)
    rule_category: str = Field(max_length=50, nullable=False)
    
    # Monitoring target
    target_metric: str = Field(max_length=100, nullable=False)
    target_table: Optional[str] = Field(max_length=50, default=None)
    target_column: Optional[str] = Field(max_length=50, default=None)
    
    # Alert condition
    condition_type: AlertCondition = Field(
        sa_column=Column(SQLEnum(AlertCondition), nullable=False)
    )
    threshold_value: Optional[float] = Field(
        sa_column=Column(Numeric(15, 4), default=None)
    )
    comparison_window_minutes: int = Field(default=5, description="Time window for comparison")
    
    # Severity and escalation
    default_severity: str = Field(max_length=10, default="warning")  # 'info', 'warning', 'critical'
    escalation_minutes: int = Field(default=15, description="Minutes before escalation")
    max_escalation_level: int = Field(default=2)
    
    # Scope and filters
    store_filter: Optional[int] = Field(default=None, description="Specific store ID (null = all stores)")
    time_of_day_filter: Optional[str] = Field(max_length=50, default=None)  # 'business_hours', 'after_hours'
    day_of_week_filter: Optional[str] = Field(max_length=20, default=None)  # 'weekdays', 'weekends'
    
    # Rule configuration
    rule_parameters: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False, default=dict),
        description="Additional parameters for complex rules"
    )
    
    # Suppression settings
    suppress_duplicate_minutes: int = Field(default=10, description="Minutes to suppress duplicate alerts")
    suppress_during_maintenance: bool = Field(default=True)
    
    # Rule status
    is_active: bool = Field(default=True)
    is_test_mode: bool = Field(default=False, description="Test mode - generate alerts but don't notify")
    
    # Notification settings
    notification_channels: Dict[str, Any] = Field(
        sa_column=Column(JSONB, nullable=False, default=dict),
        description="Notification channels and settings"
    )
    
    # Audit fields
    created_at: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("clock_timestamp()"),
                        nullable=False)
    )
    created_by: str = Field(max_length=50, nullable=False)
    last_modified_at: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("clock_timestamp()"),
                        nullable=False)
    )
    last_modified_by: str = Field(max_length=50, nullable=False)
    
    # Performance tracking
    alert_count_last_24h: int = Field(default=0, description="Alerts generated in last 24 hours")
    last_triggered_at: Optional[datetime] = Field(default=None)
    avg_resolution_minutes: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Average time to resolve alerts from this rule"
    )