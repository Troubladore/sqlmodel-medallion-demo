from datetime import datetime, date
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Boolean, Date, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.address import Address
    from src.pagila.tables.pagila.payment import Payment
    from src.pagila.tables.pagila.rental import Rental
    from src.pagila.tables.pagila.store import Store


class Customer(SQLModel, table=True):
    __tablename__ = "customer"
    __table_args__ = {"schema": "public"}

    customer_id: int = Field(primary_key=True)
    store_id: int = Field(sa_column=Column(ForeignKey("public.store.store_id"), nullable=False))
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)
    email: Optional[str] = Field(default=None, nullable=True)
    address_id: int = Field(sa_column=Column(ForeignKey("public.address.address_id"), nullable=False))
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

    # Relationships
    store: Optional["Store"] = Relationship(back_populates="customers")
    address: Optional["Address"] = Relationship(back_populates="customers")
    payments: List["Payment"] = Relationship(back_populates="customer")
    rentals: List["Rental"] = Relationship(back_populates="customer")