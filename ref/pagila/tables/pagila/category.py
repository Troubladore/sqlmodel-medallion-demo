from datetime import datetime
from typing import TYPE_CHECKING, List

from sqlalchemy import TIMESTAMP, Column, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.film_category import FilmCategory


class Category(SQLModel, table=True):
    __tablename__ = "category"
    __table_args__ = {"schema": "public"}

    category_id: int = Field(primary_key=True)
    name: str = Field(nullable=False)
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    film_categories: List["FilmCategory"] = Relationship(back_populates="category")