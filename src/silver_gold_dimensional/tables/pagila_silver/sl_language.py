from datetime import datetime
from typing import TYPE_CHECKING, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import CHAR, TIMESTAMP, Column, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila_silver_gold.tables.pagila_silver.sl_film import SlFilm


class SlLanguage(SQLModel, table=True):
    __tablename__ = "sl_language"
    __table_args__ = {"schema": "pagila_silver"}

    # Silver surrogate key
    sl_language_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    language_id: int = Field(nullable=False, unique=True)
    
    # Clean business fields
    name: str = Field(sa_column=Column(CHAR(20), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    
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

    # Relationships
    films: List["SlFilm"] = Relationship(
        back_populates="language",
        sa_relationship_kwargs={"foreign_keys": "[SlFilm.language_id]"}
    )
    original_language_films: List["SlFilm"] = Relationship(
        back_populates="original_language", 
        sa_relationship_kwargs={"foreign_keys": "[SlFilm.original_language_id]"}
    )