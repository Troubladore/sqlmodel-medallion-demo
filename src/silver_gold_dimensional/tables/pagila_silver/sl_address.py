from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.silver_gold_dimensional.tables.pagila_silver.sl_customer import SlCustomer
    from src.silver_gold_dimensional.tables.pagila_silver.sl_store import SlStore
    # from src.silver_gold_dimensional.tables.pagila_silver.sl_city import SlCity
    # from src.silver_gold_dimensional.tables.pagila_silver.sl_staff import SlStaff


class SlAddress(SQLModel, table=True):
    __tablename__ = "sl_address"
    __table_args__ = {"schema": "pagila_silver"}

    # Silver surrogate key
    sl_address_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    address_id: int = Field(nullable=False, unique=True)
    
    # Clean business fields
    address: str = Field(nullable=False, max_length=50)
    address2: Optional[str] = Field(default=None, nullable=True, max_length=50)
    district: str = Field(nullable=False, max_length=20)
    city_id: int = Field(nullable=False)  # TODO: Add FK when sl_city is created
    postal_code: Optional[str] = Field(default=None, nullable=True, max_length=10)
    phone: str = Field(nullable=False, max_length=20)
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
    sl_customer_count: Optional[int] = Field(default=0, nullable=True)
    sl_store_count: Optional[int] = Field(default=0, nullable=True)

    # Relationships
    customers: List["SlCustomer"] = Relationship(back_populates="address")
    stores: List["SlStore"] = Relationship(back_populates="address")
    # city: Optional["SlCity"] = Relationship(back_populates="addresses")  # TODO: Enable when sl_city is created
    # staff: List["SlStaff"] = Relationship(back_populates="address")  # TODO: Enable when sl_staff is created