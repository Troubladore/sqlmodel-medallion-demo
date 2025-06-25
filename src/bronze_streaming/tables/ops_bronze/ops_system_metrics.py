from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4
from sqlalchemy import TIMESTAMP, Numeric, Column, text
from sqlmodel import Field, SQLModel


class OpsSystemMetrics(SQLModel, table=True):
    """
    Real-time system performance and health metrics.
    
    Captures database performance, response times, resource utilization.
    Updated every 30 seconds via monitoring jobs.
    """
    __tablename__ = "ops_system_metrics"
    __table_args__ = {"schema": "ops_bronze"}
    
    # Primary key and timing
    metric_id: UUID = Field(default_factory=uuid4, primary_key=True)
    captured_at: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True), 
                        default=text("clock_timestamp()"),
                        nullable=False)
    )
    
    # Metric identification
    metric_name: str = Field(max_length=100, nullable=False)
    metric_category: str = Field(max_length=50, nullable=False)  # 'database', 'application', 'business'
    source_system: str = Field(max_length=50, default="pagila_db")
    
    # Metric values
    metric_value: float = Field(sa_column=Column(Numeric(15, 4), nullable=False))
    metric_unit: str = Field(max_length=20, default="count")  # 'count', 'seconds', 'bytes', 'percent'
    
    # Contextual dimensions
    store_id: Optional[int] = Field(default=None)
    table_name: Optional[str] = Field(max_length=50, default=None)
    operation_type: Optional[str] = Field(max_length=20, default=None)  # 'select', 'insert', 'update', 'delete'
    
    # Statistical context (for alerting)
    baseline_value: Optional[float] = Field(
        sa_column=Column(Numeric(15, 4), default=None),
        description="Baseline value for anomaly detection"
    )
    deviation_percent: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Percentage deviation from baseline"
    )
    
    # Alerting flags
    is_anomaly: bool = Field(default=False)
    alert_threshold_breached: bool = Field(default=False)
    severity_level: str = Field(max_length=10, default="info")  # 'info', 'warning', 'critical'
    
    # Partitioning for performance
    metric_hour: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("date_trunc('hour', clock_timestamp())"),
                        nullable=False),
        description="Hour for partitioning"
    )