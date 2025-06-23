from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.film import Film
    from src.pagila.tables.pagila.rental import Rental
    from src.pagila.tables.pagila.store import Store


class Inventory(SQLModel, table=True):
    __tablename__ = "inventory"
    __table_args__ = {"schema": "public"}

    inventory_id: int = Field(primary_key=True)
    film_id: int = Field(sa_column=Column(ForeignKey("public.film.film_id"), nullable=False))
    store_id: int = Field(sa_column=Column(ForeignKey("public.store.store_id"), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    film: Optional["Film"] = Relationship(back_populates="inventory")
    store: Optional["Store"] = Relationship(back_populates="inventory")
    rentals: List["Rental"] = Relationship(back_populates="inventory")