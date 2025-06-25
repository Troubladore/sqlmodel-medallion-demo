from datetime import datetime, date
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Boolean, Date, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.silver_gold_dimensional.tables.pagila_silver.sl_address import SlAddress
    from src.silver_gold_dimensional.tables.pagila_silver.sl_store import SlStore
    # from src.silver_gold_dimensional.tables.pagila_silver.sl_payment import SlPayment
    # from src.silver_gold_dimensional.tables.pagila_silver.sl_rental import SlRental


class SlCustomer(SQLModel, table=True):
    __tablename__ = "sl_customer"
    __table_args__ = {"schema": "pagila_silver"}

    # Silver surrogate key
    sl_customer_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    customer_id: int = Field(nullable=False, unique=True)
    
    # Clean business fields
    store_id: int = Field(sa_column=Column(ForeignKey("pagila_silver.sl_store.store_id"), nullable=False))
    first_name: str = Field(nullable=False, max_length=45)
    last_name: str = Field(nullable=False, max_length=45)
    email: Optional[str] = Field(default=None, nullable=True, max_length=50)
    address_id: int = Field(sa_column=Column(ForeignKey("pagila_silver.sl_address.address_id"), nullable=False))
    activebool: bool = Field(sa_column=Column(Boolean, nullable=False, default=True, server_default=text("true")))
    create_date: date = Field(sa_column=Column(Date, nullable=False, default=text("CURRENT_DATE"), server_default=text("CURRENT_DATE")))
    last_update: Optional[datetime] = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=True,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    active: Optional[int] = Field(default=None, nullable=True)
    
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
    sl_lifetime_value: Optional[float] = Field(default=None, nullable=True)
    sl_rental_count: Optional[int] = Field(default=0, nullable=True)
    sl_last_rental_date: Optional[date] = Field(default=None, nullable=True)

    # Relationships
    store: Optional["SlStore"] = Relationship(back_populates="customers")
    address: Optional["SlAddress"] = Relationship(back_populates="customers")
    # payments: List["SlPayment"] = Relationship(back_populates="customer")  # TODO: Enable when sl_payment is created
    # rentals: List["SlRental"] = Relationship(back_populates="customer")  # TODO: Enable when sl_rental is created