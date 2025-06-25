from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Numeric, Column, text
from sqlmodel import Field, SQLModel


class GlDimStore(SQLModel, table=True):
    __tablename__ = "gl_dim_store"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_dim_store_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    store_id: int = Field(nullable=False, unique=True)
    
    # Store attributes
    manager_staff_id: int = Field(nullable=False)
    manager_first_name: str = Field(nullable=False, max_length=45)
    manager_last_name: str = Field(nullable=False, max_length=45)
    manager_full_name: str = Field(nullable=False, max_length=91)  # first + last
    manager_email: Optional[str] = Field(default=None, nullable=True, max_length=50)
    
    # Address attributes (denormalized)
    address_line1: str = Field(nullable=False, max_length=50)
    address_line2: Optional[str] = Field(default=None, nullable=True, max_length=50)
    district: str = Field(nullable=False, max_length=20)
    city: str = Field(nullable=False, max_length=50)
    postal_code: Optional[str] = Field(default=None, nullable=True, max_length=10)
    phone: str = Field(nullable=False, max_length=20)
    country: str = Field(nullable=False, max_length=50)
    
    # Store lifecycle
    last_update: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))
    
    # Store analytics (pre-calculated)
    total_customers: int = Field(default=0, nullable=False)
    total_rentals: int = Field(default=0, nullable=False)
    total_revenue: float = Field(sa_column=Column(Numeric(12, 2), default=0.00, nullable=False))
    avg_rental_rate: Optional[float] = Field(sa_column=Column(Numeric(5, 2), nullable=True))
    total_inventory_items: int = Field(default=0, nullable=False)
    active_customers: int = Field(default=0, nullable=False)  # Customers rented in last 90 days
    
    # Store performance metrics
    revenue_per_customer: Optional[float] = Field(sa_column=Column(Numeric(8, 2), nullable=True))
    inventory_turnover_rate: Optional[float] = Field(default=None, nullable=True)
    customer_retention_rate: Optional[float] = Field(default=None, nullable=True)
    avg_rentals_per_day: Optional[float] = Field(default=None, nullable=True)
    
    # Store classification
    store_tier: Optional[str] = Field(default=None, nullable=True, max_length=20)  # High, Medium, Low performance
    is_flagship_store: bool = Field(default=False, nullable=False)
    primary_customer_segment: Optional[str] = Field(default=None, nullable=True, max_length=25)
    
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
            server_default=text("now()")
        )
    )
    gl_source_silver_key: Optional[UUID] = Field(default=None, nullable=True)
    gl_is_current: bool = Field(default=True, nullable=False)