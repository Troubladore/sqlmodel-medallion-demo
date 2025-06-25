from datetime import datetime, date
from typing import Optional
from sqlalchemy import TIMESTAMP, Numeric, Date, Column, text
from sqlmodel import Field, SQLModel


class OpsExecutiveSummary(SQLModel, table=False):
    """
    Executive-level operational summary dashboard.
    
    High-level view of business performance, alerts, and trends
    for executive decision-making and strategic oversight.
    
    SQL View Definition:
    CREATE OR REPLACE VIEW ops_gold.ops_executive_summary AS
    WITH current_metrics AS (
        SELECT 
            SUM(revenue_per_minute) as total_revenue_per_minute,
            SUM(rentals_per_minute) as total_rentals_per_minute,
            AVG(inventory_utilization_percent) as avg_inventory_utilization,
            SUM(CASE WHEN has_active_alerts THEN 1 ELSE 0 END) as stores_with_alerts,
            COUNT(*) as total_active_stores,
            MAX(last_updated) as last_data_update
        FROM ops_silver.ops_real_time_kpis 
        WHERE kpi_timestamp >= CURRENT_TIMESTAMP - INTERVAL '5 minutes'
    ),
    today_performance AS (
        SELECT 
            SUM(revenue_per_minute) as revenue_today,
            SUM(rentals_per_minute) as rentals_today,
            COUNT(DISTINCT store_id) as stores_reporting
        FROM ops_silver.ops_real_time_kpis 
        WHERE DATE(kpi_timestamp) = CURRENT_DATE
    )
    SELECT 
        CURRENT_TIMESTAMP as dashboard_timestamp,
        
        -- Current state
        cm.total_revenue_per_minute,
        cm.total_rentals_per_minute,
        cm.avg_inventory_utilization,
        cm.stores_with_alerts,
        cm.total_active_stores,
        
        -- Today's performance
        tp.revenue_today,
        tp.rentals_today,
        tp.stores_reporting,
        
        -- Health indicators
        CASE 
            WHEN cm.stores_with_alerts = 0 THEN 'Healthy'
            WHEN cm.stores_with_alerts <= cm.total_active_stores * 0.3 THEN 'Good'
            WHEN cm.stores_with_alerts <= cm.total_active_stores * 0.6 THEN 'Attention'
            ELSE 'Critical'
        END as overall_health_status,
        
        cm.last_data_update
        
    FROM current_metrics cm
    CROSS JOIN today_performance tp;
    """
    
    # Dashboard metadata
    dashboard_timestamp: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True)),
        description="When this dashboard was generated"
    )
    
    # Current business velocity
    total_revenue_per_minute: float = Field(
        sa_column=Column(Numeric(12, 2)),
        description="Company-wide revenue per minute"
    )
    total_rentals_per_minute: int = Field(description="Company-wide rentals per minute")
    avg_inventory_utilization: float = Field(
        sa_column=Column(Numeric(5, 2)),
        description="Average inventory utilization across stores"
    )
    
    # Operational health
    stores_with_alerts: int = Field(description="Number of stores with active alerts")
    total_active_stores: int = Field(description="Total number of active stores")
    overall_health_status: str = Field(max_length=20, description="Overall system health")
    
    # Today's performance summary
    revenue_today: float = Field(
        sa_column=Column(Numeric(15, 2)),
        description="Total revenue generated today"
    )
    rentals_today: int = Field(description="Total rentals processed today")
    stores_reporting: int = Field(description="Number of stores reporting data")
    
    # Trend indicators (vs yesterday)
    revenue_trend_percent: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Revenue trend vs yesterday (%)"
    )
    activity_trend_percent: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Activity trend vs yesterday (%)"
    )
    
    # Performance benchmarks
    revenue_vs_target_percent: Optional[float] = Field(
        sa_column=Column(Numeric(8, 2), default=None),
        description="Revenue vs monthly target (%)"
    )
    efficiency_score: Optional[float] = Field(
        sa_column=Column(Numeric(5, 2), default=None),
        description="Overall operational efficiency score"
    )
    
    # Data quality and freshness
    data_freshness_minutes: Optional[int] = Field(
        description="Minutes since last data update"
    )
    last_data_update: datetime = Field(
        sa_column=Column(TIMESTAMP(timezone=True)),
        description="Timestamp of most recent data"
    )