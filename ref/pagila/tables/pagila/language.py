from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import CHAR, TIMESTAMP, Column, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.film import Film


class Language(SQLModel, table=True):
    __tablename__ = "language"
    __table_args__ = {"schema": "public"}

    language_id: int = Field(primary_key=True)
    name: str = Field(sa_column=Column(CHAR(20), nullable=False))
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    films: List["Film"] = Relationship(
        back_populates="language",
        sa_relationship_kwargs={"foreign_keys": "[Film.language_id]"}
    )
    original_language_films: List["Film"] = Relationship(
        back_populates="original_language", 
        sa_relationship_kwargs={"foreign_keys": "[Film.original_language_id]"}
    )