from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.address import Address
    from src.pagila.tables.pagila.country import Country


class City(SQLModel, table=True):
    __tablename__ = "city"
    __table_args__ = {"schema": "public"}

    city_id: int = Field(primary_key=True)
    city: str = Field(nullable=False)
    country_id: int = Field(sa_column=Column(ForeignKey("public.country.country_id"), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    country: Optional["Country"] = Relationship(back_populates="cities")
    addresses: List["Address"] = Relationship(back_populates="city")