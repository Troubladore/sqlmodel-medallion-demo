from datetime import date
from typing import Optional

from sqlalchemy import Numeric, Date, Column
from sqlmodel import Field, SQLModel


class VwTopTitles(SQLModel, table=False):
    """
    Film performance mart by month.
    
    Grain: film-month
    Purpose: Content performance analysis and inventory optimization
    
    SQL Definition:
        CREATE OR REPLACE VIEW simple_gold.vw_top_titles AS
        SELECT 
            f.film_id,
            f.title,
            f.rating,
            f.rental_rate,
            DATE_TRUNC('month', r.rental_date)::date as rental_month,
            COUNT(r.rental_id) as total_rentals,
            SUM(p.amount) as total_revenue,
            AVG(p.amount) as avg_rental_revenue,
            AVG(EXTRACT(days FROM (r.return_date - r.rental_date))) as avg_days_out,
            COUNT(*) FILTER (WHERE r.return_date IS NULL) as currently_rented,
            ROUND(COUNT(r.rental_id)::numeric / COUNT(DISTINCT r.inventory_id), 2) as utilization_rate
        FROM temporal_silver.fact_rental r
        JOIN temporal_silver.dim_film f ON r.film_id = f.film_id
        JOIN temporal_silver.fact_payment p ON r.rental_id = p.rental_id
        GROUP BY f.film_id, f.title, f.rating, f.rental_rate, DATE_TRUNC('month', r.rental_date)
        ORDER BY rental_month DESC, total_rentals DESC;
    
    Business Use Cases:
        - Content popularity analysis
        - Inventory optimization decisions
        - Pricing strategy evaluation
        - Seasonal content planning
    
    Example Queries:
        -- Most popular films this quarter
        SELECT title, SUM(total_rentals) as quarterly_rentals
        FROM vw_top_titles 
        WHERE rental_month >= DATE_TRUNC('quarter', CURRENT_DATE)
        GROUP BY film_id, title
        ORDER BY quarterly_rentals DESC
        LIMIT 10;
        
        -- Revenue per rental by rating
        SELECT 
            rating,
            AVG(avg_rental_revenue) as avg_revenue_per_rental,
            AVG(avg_days_out) as avg_rental_duration
        FROM vw_top_titles
        GROUP BY rating
        ORDER BY avg_revenue_per_rental DESC;
    """
    __tablename__ = "vw_top_titles"
    __table_args__ = {"schema": "simple_gold"}

    # Grain: film-month
    film_id: int = Field(primary_key=True)
    title: str = Field(nullable=False, max_length=255)
    rating: Optional[str] = Field(default=None, nullable=True, max_length=10)
    rental_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False))
    rental_month: date = Field(sa_column=Column(Date, primary_key=True))
    
    # Measures
    total_rentals: int = Field(nullable=False)
    total_revenue: float = Field(sa_column=Column(Numeric(8, 2), nullable=False))
    avg_rental_revenue: float = Field(sa_column=Column(Numeric(6, 2), nullable=False))
    avg_days_out: Optional[float] = Field(default=None, nullable=True)
    currently_rented: int = Field(nullable=False)
    utilization_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False))