from datetime import date

from sqlalchemy import Numeric, Date, Column
from sqlmodel import Field, SQLModel


class VwMonthlyRevenue(SQLModel, table=False):
    """
    Monthly revenue mart by store.
    
    Grain: store-month
    Purpose: Executive dashboard showing revenue trends across stores
    
    SQL Definition:
        CREATE OR REPLACE VIEW simple_gold.vw_monthly_revenue AS
        SELECT 
            s.store_id,
            s.address || ', ' || s.city as store_location,
            DATE_TRUNC('month', p.payment_date)::date as revenue_month,
            SUM(p.amount) as total_revenue,
            COUNT(p.payment_id) as total_transactions,
            AVG(p.amount) as avg_transaction_amount,
            COUNT(DISTINCT p.customer_id) as unique_customers
        FROM temporal_silver.fact_payment p
        JOIN temporal_silver.dim_store s ON p.store_id = s.store_id  
        GROUP BY s.store_id, s.address, s.city, DATE_TRUNC('month', p.payment_date)
        ORDER BY s.store_id, revenue_month;
    
    Business Use Cases:
        - Store performance comparison
        - Seasonal revenue analysis
        - Growth trend identification
        - Budget vs actual tracking
    
    Example Queries:
        -- Top performing stores this year
        SELECT store_location, SUM(total_revenue) as ytd_revenue
        FROM vw_monthly_revenue 
        WHERE revenue_month >= DATE_TRUNC('year', CURRENT_DATE)
        GROUP BY store_id, store_location
        ORDER BY ytd_revenue DESC;
        
        -- Month-over-month growth
        SELECT 
            store_id,
            revenue_month,
            total_revenue,
            LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY revenue_month) as prev_month,
            (total_revenue / LAG(total_revenue) OVER (PARTITION BY store_id ORDER BY revenue_month) - 1) * 100 as growth_pct
        FROM vw_monthly_revenue
        ORDER BY store_id, revenue_month;
    """
    __tablename__ = "vw_monthly_revenue"
    __table_args__ = {"schema": "simple_gold"}

    # Grain: store-month
    store_id: int = Field(primary_key=True)
    store_location: str = Field(nullable=False, max_length=200)
    revenue_month: date = Field(sa_column=Column(Date, primary_key=True))
    
    # Measures
    total_revenue: float = Field(sa_column=Column(Numeric(10, 2), nullable=False))
    total_transactions: int = Field(nullable=False)
    avg_transaction_amount: float = Field(sa_column=Column(Numeric(6, 2), nullable=False))
    unique_customers: int = Field(nullable=False)