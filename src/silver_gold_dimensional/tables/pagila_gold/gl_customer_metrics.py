from datetime import datetime, date
from typing import Optional
from uuid import UUID

from sqlalchemy import TIMESTAMP, Date, Numeric, Column, ForeignKey, text
from sqlmodel import Field, SQLModel


class GlCustomerMetrics(SQLModel, table=True):
    __tablename__ = "gl_customer_metrics"
    __table_args__ = {"schema": "pagila_gold"}

    # Composite primary key
    customer_key: UUID = Field(
        sa_column=Column(ForeignKey("pagila_gold.gl_dim_customer.gl_dim_customer_key"), primary_key=True)
    )
    metric_date: date = Field(sa_column=Column(Date, primary_key=True))
    
    # Customer identifier
    customer_id: int = Field(nullable=False)
    
    # Rental metrics
    rentals_to_date: int = Field(default=0, nullable=False)
    rentals_current_month: int = Field(default=0, nullable=False)
    rentals_current_quarter: int = Field(default=0, nullable=False)
    rentals_current_year: int = Field(default=0, nullable=False)
    
    # Payment metrics
    total_payments: float = Field(sa_column=Column(Numeric(8, 2), default=0.00, nullable=False))
    payments_current_month: float = Field(sa_column=Column(Numeric(6, 2), default=0.00, nullable=False))
    payments_current_quarter: float = Field(sa_column=Column(Numeric(7, 2), default=0.00, nullable=False))
    payments_current_year: float = Field(sa_column=Column(Numeric(8, 2), default=0.00, nullable=False))
    
    # Average metrics
    avg_payment_amount: Optional[float] = Field(sa_column=Column(Numeric(5, 2), nullable=True))
    avg_rental_frequency_monthly: Optional[float] = Field(default=None, nullable=True)  # Rentals per month
    avg_days_between_rentals: Optional[float] = Field(default=None, nullable=True)
    
    # Lifecycle metrics
    customer_age_days: int = Field(nullable=False)  # Days since first rental
    days_since_last_rental: Optional[int] = Field(default=None, nullable=True)
    is_active_customer: bool = Field(default=True, nullable=False)  # Rented in last 90 days
    
    # Customer value metrics
    customer_lifetime_value: float = Field(sa_column=Column(Numeric(8, 2), default=0.00, nullable=False))
    customer_value_rank: Optional[int] = Field(default=None, nullable=True)  # Rank among all customers
    customer_value_percentile: Optional[int] = Field(default=None, nullable=True)  # 1-100
    
    # Behavioral metrics
    preferred_rental_day: Optional[str] = Field(default=None, nullable=True, max_length=9)  # Monday, etc.
    preferred_rental_hour: Optional[int] = Field(default=None, nullable=True)  # 0-23
    most_rented_genre: Optional[str] = Field(default=None, nullable=True, max_length=25)
    genre_diversity_score: Optional[float] = Field(default=None, nullable=True)  # 0-1
    
    # Return behavior
    avg_return_delay_days: Optional[float] = Field(default=None, nullable=True)
    late_return_rate: Optional[float] = Field(default=None, nullable=True)  # 0-1
    total_late_returns: int = Field(default=0, nullable=False)
    
    # Risk metrics
    overdue_rentals_current: int = Field(default=0, nullable=False)
    total_overdue_fees: float = Field(sa_column=Column(Numeric(6, 2), default=0.00, nullable=False))
    credit_score: Optional[int] = Field(default=None, nullable=True)  # 1-100 internal score
    
    # Engagement metrics
    store_loyalty_score: Optional[float] = Field(default=None, nullable=True)  # 0-1
    seasonal_activity_pattern: Optional[str] = Field(default=None, nullable=True, max_length=50)
    
    # Prediction scores
    churn_risk_score: Optional[float] = Field(default=None, nullable=True)  # 0-1
    upsell_opportunity_score: Optional[float] = Field(default=None, nullable=True)  # 0-1
    next_rental_prediction_days: Optional[int] = Field(default=None, nullable=True)
    
    # Gold audit fields
    gl_created_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    gl_updated_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()"))) 
    gl_calculation_version: str = Field(default="1.0", nullable=False, max_length=10)