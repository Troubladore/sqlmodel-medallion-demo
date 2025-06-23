from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Numeric, Column, text
from sqlmodel import Field, SQLModel


class GlDimStaff(SQLModel, table=True):
    __tablename__ = "gl_dim_staff"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_dim_staff_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    staff_id: int = Field(nullable=False, unique=True)
    
    # Staff attributes
    first_name: str = Field(nullable=False, max_length=45)
    last_name: str = Field(nullable=False, max_length=45)
    full_name: str = Field(nullable=False, max_length=91)  # first + last
    email: Optional[str] = Field(default=None, nullable=True, max_length=50)
    username: str = Field(nullable=False, max_length=16)
    is_active: bool = Field(default=True, nullable=False)
    
    # Store association
    store_id: int = Field(nullable=False)
    store_address: str = Field(nullable=False, max_length=100)
    store_city: str = Field(nullable=False, max_length=50)
    is_store_manager: bool = Field(default=False, nullable=False)
    
    # Address attributes (denormalized)
    address_line1: str = Field(nullable=False, max_length=50)
    address_line2: Optional[str] = Field(default=None, nullable=True, max_length=50)
    district: str = Field(nullable=False, max_length=20)
    city: str = Field(nullable=False, max_length=50)
    postal_code: Optional[str] = Field(default=None, nullable=True, max_length=10)
    phone: str = Field(nullable=False, max_length=20)
    country: str = Field(nullable=False, max_length=50)
    
    # Staff lifecycle
    last_update: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))
    
    # Staff performance analytics (pre-calculated)
    total_rentals_processed: int = Field(default=0, nullable=False)
    total_payments_processed: int = Field(default=0, nullable=False)
    total_payment_amount: float = Field(sa_column=Column(Numeric(12, 2), default=0.00, nullable=False))
    avg_payment_amount: Optional[float] = Field(sa_column=Column(Numeric(6, 2), nullable=True))
    
    # Staff efficiency metrics
    avg_rentals_per_day: Optional[float] = Field(default=None, nullable=True)
    avg_payments_per_day: Optional[float] = Field(default=None, nullable=True)
    customer_satisfaction_score: Optional[float] = Field(default=None, nullable=True)  # 1-5 scale
    
    # Staff classification
    performance_tier: Optional[str] = Field(default=None, nullable=True, max_length=20)  # Top, High, Average, Low
    is_top_performer: bool = Field(default=False, nullable=False)
    specialization: Optional[str] = Field(default=None, nullable=True, max_length=25)  # Customer Service, Sales, etc.
    
    # Manager-specific metrics (only populated if is_store_manager=True)
    managed_staff_count: Optional[int] = Field(default=None, nullable=True)
    store_revenue_under_management: Optional[float] = Field(sa_column=Column(Numeric(12, 2), nullable=True))
    team_performance_score: Optional[float] = Field(default=None, nullable=True)
    
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