from datetime import datetime, date
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Date, Numeric, Column, ForeignKey, text
from sqlmodel import Field, SQLModel


class GlFactRental(SQLModel, table=True):
    __tablename__ = "gl_fact_rental"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_fact_rental_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    rental_id: int = Field(nullable=False, unique=True)
    
    # Dimension foreign keys
    customer_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_customer.gl_dim_customer_key"), nullable=False))
    film_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_film.gl_dim_film_key"), nullable=False))
    store_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_store.gl_dim_store_key"), nullable=False))
    staff_key: UUID = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_staff.gl_dim_staff_key"), nullable=False))
    rental_date_key: int = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_date.date_key"), nullable=False))
    return_date_key: Optional[int] = Field(sa_column=Column(ForeignKey("pagila_gold.gl_dim_date.date_key"), nullable=True))
    
    # Date/Time dimensions
    rental_date: date = Field(sa_column=Column(Date, nullable=False))
    rental_datetime: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))
    return_date: Optional[date] = Field(sa_column=Column(Date, nullable=True))
    return_datetime: Optional[datetime] = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=True))
    
    # Measures and metrics
    rental_duration_actual: Optional[int] = Field(default=None, nullable=True)  # Days
    rental_duration_planned: int = Field(nullable=False)  # From film
    rental_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False))
    replacement_cost: float = Field(sa_column=Column(Numeric(5, 2), nullable=False))
    
    # Calculated measures
    is_overdue: bool = Field(default=False, nullable=False)
    days_overdue: Optional[int] = Field(default=None, nullable=True)
    is_returned: bool = Field(default=False, nullable=False)
    revenue_amount: Optional[float] = Field(sa_column=Column(Numeric(5, 2), nullable=True))
    
    # Flags for analytics
    is_weekend_rental: bool = Field(default=False, nullable=False)
    is_holiday_rental: bool = Field(default=False, nullable=False)
    rental_hour: int = Field(nullable=False)  # 0-23
    rental_quarter: int = Field(nullable=False)  # 1-4
    rental_month: int = Field(nullable=False)  # 1-12
    rental_year: int = Field(nullable=False)
    
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
    gl_source_silver_key: Optional[UUID] = Field(default=None, nullable=True)