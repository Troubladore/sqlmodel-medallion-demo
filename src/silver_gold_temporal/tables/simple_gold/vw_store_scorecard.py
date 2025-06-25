from datetime import date
from typing import Optional

from sqlalchemy import Numeric, Date, Column
from sqlmodel import Field, SQLModel


class VwStoreScorecard(SQLModel, table=False):
    """
    Daily store performance scorecard.
    
    Grain: store-day
    Purpose: Operational dashboard for store managers and regional directors
    
    SQL Definition:
        CREATE OR REPLACE VIEW simple_gold.vw_store_scorecard AS
        SELECT 
            s.store_id,
            s.address || ', ' || s.city as store_location,
            s.manager_name,
            r.rental_date::date as business_date,
            
            -- Revenue metrics
            COALESCE(SUM(p.amount), 0) as daily_revenue,
            COUNT(DISTINCT r.rental_id) as total_rentals,
            COUNT(DISTINCT r.customer_id) as active_customers,
            
            -- Operational metrics
            COUNT(*) FILTER (WHERE r.return_date IS NOT NULL 
                AND r.return_date::date > (r.rental_date::date + f.rental_duration)) as late_returns,
            COUNT(*) FILTER (WHERE r.return_date IS NOT NULL) as completed_rentals,
            ROUND(
                COUNT(*) FILTER (WHERE r.return_date IS NOT NULL 
                    AND r.return_date::date > (r.rental_date::date + f.rental_duration))::numeric 
                / NULLIF(COUNT(*) FILTER (WHERE r.return_date IS NOT NULL), 0) * 100, 2
            ) as late_return_pct,
            
            -- Inventory metrics
            COUNT(DISTINCT r.inventory_id) as unique_titles_rented,
            ROUND(AVG(f.rental_rate), 2) as avg_rental_rate
            
        FROM temporal_silver.fact_rental r
        JOIN temporal_silver.dim_store s ON r.store_id = s.store_id
        JOIN temporal_silver.dim_film f ON r.film_id = f.film_id
        LEFT JOIN temporal_silver.fact_payment p ON r.rental_id = p.rental_id
        GROUP BY s.store_id, s.address, s.city, s.manager_name, r.rental_date::date
        ORDER BY business_date DESC, store_id;
    
    Business Use Cases:
        - Daily operations monitoring
        - Store manager performance tracking
        - Late return trend analysis
        - Customer engagement measurement
    
    Example Queries:
        -- Store performance comparison (last 30 days)
        SELECT 
            store_location,
            AVG(daily_revenue) as avg_daily_revenue,
            AVG(active_customers) as avg_daily_customers,
            AVG(late_return_pct) as avg_late_return_pct
        FROM vw_store_scorecard 
        WHERE business_date >= CURRENT_DATE - 30
        GROUP BY store_id, store_location
        ORDER BY avg_daily_revenue DESC;
        
        -- Trend analysis
        SELECT 
            business_date,
            SUM(daily_revenue) as chain_revenue,
            SUM(active_customers) as chain_customers,
            AVG(late_return_pct) as chain_late_return_pct
        FROM vw_store_scorecard
        WHERE business_date >= CURRENT_DATE - 90
        GROUP BY business_date
        ORDER BY business_date;
    """
    __tablename__ = "vw_store_scorecard"
    __table_args__ = {"schema": "simple_gold"}

    # Grain: store-day
    store_id: int = Field(primary_key=True)
    store_location: str = Field(nullable=False, max_length=200)
    manager_name: str = Field(nullable=False, max_length=90)
    business_date: date = Field(sa_column=Column(Date, primary_key=True))
    
    # Revenue measures
    daily_revenue: float = Field(sa_column=Column(Numeric(8, 2), nullable=False))
    total_rentals: int = Field(nullable=False)
    active_customers: int = Field(nullable=False)
    
    # Operational measures
    late_returns: int = Field(nullable=False)
    completed_rentals: int = Field(nullable=False)
    late_return_pct: Optional[float] = Field(sa_column=Column(Numeric(5, 2), nullable=True))
    
    # Inventory measures
    unique_titles_rented: int = Field(nullable=False)
    avg_rental_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False))