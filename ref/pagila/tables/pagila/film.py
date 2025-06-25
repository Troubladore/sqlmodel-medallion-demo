from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from enum import Enum

from sqlalchemy import (
    ARRAY, TIMESTAMP, SMALLINT, Column, ForeignKey, Numeric, Text,
    Enum as SQLEnum, text
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.film_actor import FilmActor
    from src.pagila.tables.pagila.film_category import FilmCategory
    from src.pagila.tables.pagila.inventory import Inventory
    from src.pagila.tables.pagila.language import Language


class MPAARating(str, Enum):
    G = "G"
    PG = "PG"
    PG_13 = "PG-13"
    R = "R"
    NC_17 = "NC-17"


class Film(SQLModel, table=True):
    __tablename__ = "film"
    __table_args__ = {"schema": "public"}

    film_id: int = Field(primary_key=True)
    title: str = Field(nullable=False)
    description: Optional[str] = Field(default=None, nullable=True)
    release_year: Optional[int] = Field(default=None, nullable=True)
    language_id: int = Field(sa_column=Column(ForeignKey("public.language.language_id"), nullable=False))
    original_language_id: Optional[int] = Field(
        sa_column=Column(ForeignKey("public.language.language_id"), nullable=True),
        default=None
    )
    rental_duration: int = Field(sa_column=Column(SMALLINT, nullable=False, default=3, server_default=text("3")))
    rental_rate: float = Field(
        sa_column=Column(Numeric(4, 2), nullable=False, default=4.99, server_default=text("4.99"))
    )
    length: Optional[int] = Field(sa_column=Column(SMALLINT, nullable=True), default=None)
    replacement_cost: float = Field(
        sa_column=Column(Numeric(5, 2), nullable=False, default=19.99, server_default=text("19.99"))
    )
    rating: Optional[MPAARating] = Field(
        sa_column=Column(
            SQLEnum(MPAARating, name="mpaa_rating"),
            nullable=True,
            default=MPAARating.G,
            server_default=text("'G'::mpaa_rating")
        )
    )
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    special_features: Optional[List[str]] = Field(
        sa_column=Column(ARRAY(Text), nullable=True),
        default=None
    )
    fulltext: str = Field(sa_column=Column(TSVECTOR, nullable=False))

    # Relationships
    language: Optional["Language"] = Relationship(
        back_populates="films",
        sa_relationship_kwargs={"foreign_keys": "[Film.language_id]"}
    )
    original_language: Optional["Language"] = Relationship(
        back_populates="original_language_films",
        sa_relationship_kwargs={"foreign_keys": "[Film.original_language_id]"}
    )
    film_actors: List["FilmActor"] = Relationship(back_populates="film")
    film_categories: List["FilmCategory"] = Relationship(back_populates="film")
    inventory: List["Inventory"] = Relationship(back_populates="film")