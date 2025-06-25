from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, Numeric
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.customer import Customer
    from src.pagila.tables.pagila.rental import Rental
    from src.pagila.tables.pagila.staff import Staff


class Payment(SQLModel, table=True):
    __tablename__ = "payment"
    __table_args__ = {"schema": "public"}

    payment_id: int = Field(primary_key=True)
    customer_id: int = Field(sa_column=Column(ForeignKey("public.customer.customer_id"), nullable=False))
    staff_id: int = Field(sa_column=Column(ForeignKey("public.staff.staff_id"), nullable=False))
    rental_id: int = Field(sa_column=Column(ForeignKey("public.rental.rental_id"), nullable=False))
    amount: float = Field(sa_column=Column(Numeric(5, 2), nullable=False))
    payment_date: datetime = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=False))

    # Relationships
    customer: Optional["Customer"] = Relationship(back_populates="payments")
    staff: Optional["Staff"] = Relationship(back_populates="payments")
    rental: Optional["Rental"] = Relationship(back_populates="payments")