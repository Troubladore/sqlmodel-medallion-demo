from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4
from enum import Enum

from sqlalchemy import (
    ARRAY, TIMESTAMP, SMALLINT, Column, ForeignKey, Numeric, 
    Enum as SQLEnum, Text, text
)
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila_silver_gold.tables.pagila_silver.sl_film_actor import SlFilmActor
    from src.pagila_silver_gold.tables.pagila_silver.sl_film_category import SlFilmCategory
    from src.pagila_silver_gold.tables.pagila_silver.sl_inventory import SlInventory
    from src.pagila_silver_gold.tables.pagila_silver.sl_language import SlLanguage


class MPAARating(str, Enum):
    G = "G"
    PG = "PG"
    PG_13 = "PG-13"
    R = "R"
    NC_17 = "NC-17"


class SlFilm(SQLModel, table=True):
    __tablename__ = "sl_film"
    __table_args__ = {"schema": "pagila_silver"}

    # Silver surrogate key
    sl_film_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    film_id: int = Field(nullable=False, unique=True)
    
    # Clean business fields
    title: str = Field(nullable=False, max_length=255)
    description: Optional[str] = Field(default=None, nullable=True)
    release_year: Optional[int] = Field(default=None, nullable=True)
    language_id: int = Field(sa_column=Column(ForeignKey("pagila_silver.sl_language.language_id"), nullable=False))
    original_language_id: Optional[int] = Field(
        sa_column=Column(ForeignKey("pagila_silver.sl_language.language_id"), nullable=True),
        default=None
    )
    rental_duration: int = Field(sa_column=Column(SMALLINT, nullable=False, default=3, server_default=text("3")))
    rental_rate: float = Field(
        sa_column=Column(Numeric(4, 2), nullable=False, default=4.99, server_default=text("4.99"))
    )
    length: Optional[int] = Field(sa_column=Column(SMALLINT, nullable=True))
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
        sa_column=Column(ARRAY(Text), nullable=True)
    )
    fulltext: str = Field(sa_column=Column(TSVECTOR, nullable=False))
    
    # Silver audit fields
    sl_created_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    sl_updated_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    sl_source_bronze_key: Optional[UUID] = Field(default=None, nullable=True)
    sl_is_active: bool = Field(default=True, nullable=False)
    sl_data_quality_score: Optional[float] = Field(default=None, nullable=True)
    
    # Silver business metrics
    sl_rental_count: Optional[int] = Field(default=0, nullable=True)
    sl_total_revenue: Optional[float] = Field(default=0.0, nullable=True)
    sl_avg_rental_duration: Optional[float] = Field(default=None, nullable=True)

    # Relationships
    language: Optional["SlLanguage"] = Relationship(
        back_populates="films",
        sa_relationship_kwargs={"foreign_keys": "[SlFilm.language_id]"}
    )
    original_language: Optional["SlLanguage"] = Relationship(
        back_populates="original_language_films",
        sa_relationship_kwargs={"foreign_keys": "[SlFilm.original_language_id]"}
    )
    film_actors: List["SlFilmActor"] = Relationship(back_populates="film")
    film_categories: List["SlFilmCategory"] = Relationship(back_populates="film")
    inventory: List["SlInventory"] = Relationship(back_populates="film")