from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.silver_gold_dimensional.tables.pagila_silver.sl_address import SlAddress
    from src.silver_gold_dimensional.tables.pagila_silver.sl_customer import SlCustomer
    # from src.silver_gold_dimensional.tables.pagila_silver.sl_staff import SlStaff


class SlStore(SQLModel, table=True):
    __tablename__ = "sl_store"
    __table_args__ = {"schema": "pagila_silver"}

    # Silver surrogate key
    sl_store_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    store_id: int = Field(nullable=False, unique=True)
    
    # Clean business fields
    manager_staff_id: int = Field(nullable=False)  # TODO: Add FK when sl_staff is created
    address_id: int = Field(sa_column=Column(ForeignKey("pagila_silver.sl_address.address_id"), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    
    # Silver audit fields
    sl_created_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    sl_updated_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    sl_source_bronze_key: Optional[UUID] = Field(default=None, nullable=True)
    sl_is_active: bool = Field(default=True, nullable=False)
    sl_data_quality_score: Optional[float] = Field(default=None, nullable=True)
    
    # Silver business metrics
    sl_total_revenue: Optional[float] = Field(default=0.0, nullable=True)
    sl_total_customers: Optional[int] = Field(default=0, nullable=True)
    sl_total_rentals: Optional[int] = Field(default=0, nullable=True)
    sl_avg_rental_rate: Optional[float] = Field(default=None, nullable=True)

    # Relationships
    address: Optional["SlAddress"] = Relationship(back_populates="stores")
    customers: List["SlCustomer"] = Relationship(back_populates="store")
    # staff: List["SlStaff"] = Relationship(back_populates="store")  # TODO: Enable when sl_staff is created