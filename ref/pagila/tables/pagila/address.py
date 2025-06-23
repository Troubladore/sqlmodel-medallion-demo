from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.city import City
    from src.pagila.tables.pagila.customer import Customer
    from src.pagila.tables.pagila.staff import Staff
    from src.pagila.tables.pagila.store import Store


class Address(SQLModel, table=True):
    __tablename__ = "address"
    __table_args__ = {"schema": "public"}

    address_id: int = Field(primary_key=True)
    address: str = Field(nullable=False)
    address2: Optional[str] = Field(default=None, nullable=True)
    district: str = Field(nullable=False)
    city_id: int = Field(sa_column=Column(ForeignKey("public.city.city_id"), nullable=False))
    postal_code: Optional[str] = Field(default=None, nullable=True)
    phone: str = Field(nullable=False)
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    city: Optional["City"] = Relationship(back_populates="addresses")
    customers: List["Customer"] = Relationship(back_populates="address")
    staff: List["Staff"] = Relationship(back_populates="address")
    stores: List["Store"] = Relationship(back_populates="address")