from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Boolean, LargeBinary, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.address import Address
    from src.pagila.tables.pagila.payment import Payment
    from src.pagila.tables.pagila.rental import Rental
    from src.pagila.tables.pagila.store import Store


class Staff(SQLModel, table=True):
    __tablename__ = "staff"
    __table_args__ = {"schema": "public"}

    staff_id: int = Field(primary_key=True)
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)
    address_id: int = Field(sa_column=Column(ForeignKey("public.address.address_id"), nullable=False))
    email: Optional[str] = Field(default=None, nullable=True)
    store_id: int = Field(sa_column=Column(ForeignKey("public.store.store_id"), nullable=False))
    active: bool = Field(sa_column=Column(Boolean, nullable=False, default=True, server_default=text("true")))
    username: str = Field(nullable=False)
    password: Optional[str] = Field(default=None, nullable=True)
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    picture: Optional[bytes] = Field(sa_column=Column(LargeBinary, nullable=True), default=None)

    # Relationships
    address: Optional["Address"] = Relationship(back_populates="staff")
    store: Optional["Store"] = Relationship(back_populates="staff", sa_relationship_kwargs={"foreign_keys": "[Staff.store_id]"})
    managed_stores: List["Store"] = Relationship(back_populates="manager", sa_relationship_kwargs={"foreign_keys": "[Store.manager_staff_id]"})
    payments: List["Payment"] = Relationship(back_populates="staff")
    rentals: List["Rental"] = Relationship(back_populates="staff")