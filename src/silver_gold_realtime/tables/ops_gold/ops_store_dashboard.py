from datetime import datetime
from typing import Optional
from sqlalchemy import TIMESTAMP, Numeric, Date, Column, text
from sqlmodel import Field, SQLModel


class OpsStoreDashboard(SQLModel, table=False):
    """
    Live operational dashboard for store managers.
    
    Real-time view combining current KPIs, alerts, and performance metrics
    for day-to-day store operations management.
    
    SQL View Definition:
    CREATE OR REPLACE VIEW ops_gold.ops_store_dashboard AS
    SELECT 
        s.store_id,
        s.store_address,
        s.manager_name,
        
        -- Current activity (last 5 minutes)
        COALESCE(k.rentals_per_minute, 0) as current_rentals_per_minute,
        COALESCE(k.revenue_per_minute, 0) as current_revenue_per_minute,
        COALESCE(k.active_customers_count, 0) as active_customers,
        
        -- Today's performance
        SUM(COALESCE(k.revenue_per_minute, 0)) OVER (
            PARTITION BY s.store_id, DATE(k.kpi_timestamp)
            ORDER BY k.kpi_timestamp
            ROWS UNBOUNDED PRECEDING
        ) as revenue_today,
        
        -- Alerts and issues
        COALESCE(k.has_active_alerts, false) as has_alerts,
        COALESCE(k.critical_alert_count, 0) as critical_alerts,
        COALESCE(k.warning_alert_count, 0) as warning_alerts,
        
        -- Performance indicators
        COALESCE(k.inventory_utilization_percent, 0) as inventory_utilization,
        COALESCE(k.avg_response_time_ms, 0) as system_response_time,
        COALESCE(k.error_rate_percent, 0) as error_rate,
        
        -- Comparative performance
        COALESCE(k.revenue_vs_baseline_percent, 0) as revenue_vs_baseline,
        COALESCE(k.activity_vs_baseline_percent, 0) as activity_vs_baseline,
        
        -- Data quality
        COALESCE(k.data_completeness_percent, 100) as data_quality,
        COALESCE(k.last_updated, CURRENT_TIMESTAMP) as last_updated
        
    FROM ops_gold.store_master s
    LEFT JOIN ops_silver.ops_real_time_kpis k 
        ON s.store_id = k.store_id 
        AND k.kpi_timestamp >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
    WHERE s.is_active = true
    ORDER BY s.store_id;
    """
    
    # Store identification
    store_id: int = Field(primary_key=True)
    store_address: str = Field(max_length=200)
    manager_name: str = Field(max_length=100)
    
    # Current activity (last 5 minutes)
    current_rentals_per_minute: int = Field(description="Rentals in last minute")
    current_revenue_per_minute: float = Field(
        sa_column=Column(Numeric(10, 2)),
        description="Revenue in last minute"
    )
    active_customers: int = Field(description="Customers currently in store")
    
    # Today's cumulative performance
    revenue_today: float = Field(
        sa_column=Column(Numeric(12, 2)),
        description="Total revenue today"
    )
    transactions_today: int = Field(description="Total transactions today")
    customers_served_today: int = Field(description="Unique customers served today")
    
    # Alert status
    has_alerts: bool = Field(description="Store has active alerts")
    critical_alerts: int = Field(description="Count of critical alerts")
    warning_alerts: int = Field(description="Count of warning alerts")
    
    # Operational efficiency
    inventory_utilization: float = Field(
        sa_column=Column(Numeric(5, 2)),
        description="Percentage of inventory rented out"
    )
    system_response_time: float = Field(
        sa_column=Column(Numeric(8, 2)),
        description="Average system response time (ms)"
    )
    error_rate: float = Field(
        sa_column=Column(Numeric(5, 2)),
        description="Error rate percentage"
    )
    
    # Performance comparison
    revenue_vs_baseline: float = Field(
        sa_column=Column(Numeric(8, 2)),
        description="Revenue performance vs baseline (%)"
    )
    activity_vs_baseline: float = Field(
        sa_column=Column(Numeric(8, 2)),
        description="Activity level vs baseline (%)"
    )
    
    # Data quality indicators
    data_quality: float = Field(
        sa_column=Column(Numeric(5, 2)),
        description="Data completeness percentage"
    )
    last_updated: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True)),
        description="Last data update timestamp"
    )