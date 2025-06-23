from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.category import Category
    from src.pagila.tables.pagila.film import Film


class FilmCategory(SQLModel, table=True):
    __tablename__ = "film_category"
    __table_args__ = {"schema": "public"}

    film_id: int = Field(
        sa_column=Column(ForeignKey("public.film.film_id"), primary_key=True)
    )
    category_id: int = Field(
        sa_column=Column(ForeignKey("public.category.category_id"), primary_key=True)
    )
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    film: Optional["Film"] = Relationship(back_populates="film_categories")
    category: Optional["Category"] = Relationship(back_populates="film_categories")