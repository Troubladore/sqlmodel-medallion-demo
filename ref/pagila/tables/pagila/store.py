from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.address import Address
    from src.pagila.tables.pagila.customer import Customer
    from src.pagila.tables.pagila.inventory import Inventory
    from src.pagila.tables.pagila.staff import Staff


class Store(SQLModel, table=True):
    __tablename__ = "store"
    __table_args__ = {"schema": "public"}

    store_id: int = Field(primary_key=True)
    manager_staff_id: int = Field(sa_column=Column(ForeignKey("public.staff.staff_id"), nullable=False))
    address_id: int = Field(sa_column=Column(ForeignKey("public.address.address_id"), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    manager: Optional["Staff"] = Relationship(back_populates="managed_stores", sa_relationship_kwargs={"foreign_keys": "[Store.manager_staff_id]"})
    address: Optional["Address"] = Relationship(back_populates="stores")
    staff: List["Staff"] = Relationship(back_populates="store", sa_relationship_kwargs={"foreign_keys": "[Staff.store_id]"})
    customers: List["Customer"] = Relationship(back_populates="store")
    inventory: List["Inventory"] = Relationship(back_populates="store")