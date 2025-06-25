from datetime import datetime
from typing import Optional
from sqlalchemy import TIMESTAMP, Numeric, Column, text
from sqlmodel import Field, SQLModel


class OpsRealTimeKPIs(SQLModel, table=True):
    """
    Real-time operational KPIs aggregated from business events.
    
    Materialized view refreshed every 30 seconds with current business metrics.
    Provides up-to-the-minute operational intelligence.
    """
    __tablename__ = "ops_real_time_kpis"
    __table_args__ = {"schema": "ops_silver"}
    
    # Time dimension (primary key component)
    kpi_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("date_trunc('minute', clock_timestamp())"),
                        primary_key=True),
        description="Minute-level aggregation timestamp"
    )
    
    # Dimensional breakdown
    store_id: Optional[int] = Field(primary_key=True, default=0, description="Store ID (0 = all stores)")
    
    # Business velocity metrics (per minute)
    rentals_per_minute: int = Field(default=0)
    returns_per_minute: int = Field(default=0)
    payments_per_minute: int = Field(default=0)
    new_customers_per_minute: int = Field(default=0)
    
    # Revenue metrics (per minute)
    revenue_per_minute: float = Field(
        sa_column=Column(Numeric(10, 2), default=0.00),
        description="Revenue generated in this minute"
    )
    avg_transaction_value: float = Field(
        sa_column=Column(Numeric(8, 2), default=0.00),
        description="Average transaction value this minute"
    )
    
    # Operational efficiency
    avg_rental_duration_minutes: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Average rental processing time"
    )
    inventory_utilization_percent: float = Field(
        sa_column=Column(Numeric(5, 2), default=0.00),
        description="Percentage of inventory currently rented"
    )
    
    # Customer experience metrics
    active_customers_count: int = Field(default=0, description="Customers active this minute")
    customer_wait_time_seconds: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Average customer wait time"
    )
    
    # System performance
    avg_response_time_ms: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Average system response time"
    )
    error_rate_percent: float = Field(
        sa_column=Column(Numeric(5, 2), default=0.00),
        description="Error rate percentage"
    )
    
    # Comparative metrics (vs. baseline)
    revenue_vs_baseline_percent: float = Field(
        sa_column=Column(Numeric(8, 2), default=0.00),
        description="Revenue performance vs baseline"
    )
    activity_vs_baseline_percent: float = Field(
        sa_column=Column(Numeric(8, 2), default=0.00),
        description="Activity level vs baseline"
    )
    
    # Alert indicators
    has_active_alerts: bool = Field(default=False)
    critical_alert_count: int = Field(default=0)
    warning_alert_count: int = Field(default=0)
    
    # Data quality
    data_completeness_percent: float = Field(
        sa_column=Column(Numeric(5, 2), default=100.00),
        description="Percentage of expected data received"
    )
    last_updated: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True),
                        default=text("clock_timestamp()"),
                        nullable=False)
    )