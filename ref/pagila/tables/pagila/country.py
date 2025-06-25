from datetime import datetime
from typing import TYPE_CHECKING, List

from sqlalchemy import TIMESTAMP, Column, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.city import City


class Country(SQLModel, table=True):
    __tablename__ = "country"
    __table_args__ = {"schema": "public"}

    country_id: int = Field(primary_key=True)
    country: str = Field(nullable=False)
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    cities: List["City"] = Relationship(back_populates="country")