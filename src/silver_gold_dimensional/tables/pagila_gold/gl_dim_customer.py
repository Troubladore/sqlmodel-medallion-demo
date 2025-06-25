from datetime import datetime, date
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Date, Numeric, Column, text
from sqlmodel import Field, SQLModel


class GlDimCustomer(SQLModel, table=True):
    __tablename__ = "gl_dim_customer"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_dim_customer_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    customer_id: int = Field(nullable=False, unique=True)
    
    # Customer attributes
    first_name: str = Field(nullable=False, max_length=45)
    last_name: str = Field(nullable=False, max_length=45)
    full_name: str = Field(nullable=False, max_length=91)  # first + last
    email: Optional[str] = Field(default=None, nullable=True, max_length=50)
    
    # Address attributes (denormalized)
    address_line1: str = Field(nullable=False, max_length=50)
    address_line2: Optional[str] = Field(default=None, nullable=True, max_length=50)
    district: str = Field(nullable=False, max_length=20)
    city: str = Field(nullable=False, max_length=50)
    postal_code: Optional[str] = Field(default=None, nullable=True, max_length=10)
    phone: str = Field(nullable=False, max_length=20)
    country: str = Field(nullable=False, max_length=50)
    
    # Store attributes
    store_id: int = Field(nullable=False)
    store_address: str = Field(nullable=False, max_length=100)
    store_city: str = Field(nullable=False, max_length=50)
    
    # Customer lifecycle
    create_date: date = Field(sa_column=Column(Date, nullable=False))
    is_active: bool = Field(default=True, nullable=False)
    last_update: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))
    
    # Customer analytics (pre-calculated)
    total_rentals: int = Field(default=0, nullable=False)
    total_payments: float = Field(sa_column=Column(Numeric(8, 2), default=0.00, nullable=False))
    avg_payment_amount: Optional[float] = Field(sa_column=Column(Numeric(5, 2), nullable=True))
    first_rental_date: Optional[date] = Field(sa_column=Column(Date, nullable=True))
    last_rental_date: Optional[date] = Field(sa_column=Column(Date, nullable=True))
    days_since_last_rental: Optional[int] = Field(default=None, nullable=True)
    
    # Customer segmentation
    customer_lifetime_value: Optional[float] = Field(sa_column=Column(Numeric(8, 2), nullable=True))
    customer_tier: Optional[str] = Field(default=None, nullable=True, max_length=20)  # Bronze, Silver, Gold, Platinum
    is_high_value: bool = Field(default=False, nullable=False)
    preferred_genre: Optional[str] = Field(default=None, nullable=True, max_length=25)
    avg_rental_frequency: Optional[float] = Field(default=None, nullable=True)  # Rentals per month
    
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
    gl_is_current: bool = Field(default=True, nullable=False)