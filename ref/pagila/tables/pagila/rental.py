from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.customer import Customer
    from src.pagila.tables.pagila.inventory import Inventory
    from src.pagila.tables.pagila.payment import Payment
    from src.pagila.tables.pagila.staff import Staff


class Rental(SQLModel, table=True):
    __tablename__ = "rental"
    __table_args__ = {"schema": "public"}

    rental_id: int = Field(primary_key=True)
    rental_date: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))
    inventory_id: int = Field(sa_column=Column(ForeignKey("public.inventory.inventory_id"), nullable=False))
    customer_id: int = Field(sa_column=Column(ForeignKey("public.customer.customer_id"), nullable=False))
    return_date: Optional[datetime] = Field(
        sa_column=Column(TIMESTAMP(timezone=True), nullable=True),
        default=None
    )
    staff_id: int = Field(sa_column=Column(ForeignKey("public.staff.staff_id"), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    inventory: Optional["Inventory"] = Relationship(back_populates="rentals")
    customer: Optional["Customer"] = Relationship(back_populates="rentals")
    staff: Optional["Staff"] = Relationship(back_populates="rentals")
    payments: List["Payment"] = Relationship(back_populates="rental")