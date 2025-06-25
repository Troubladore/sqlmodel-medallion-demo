from datetime import datetime
from typing import TYPE_CHECKING, List

from sqlalchemy import TIMESTAMP, Column, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.film_actor import FilmActor


class Actor(SQLModel, table=True):
    __tablename__ = "actor"
    __table_args__ = {"schema": "public"}

    actor_id: int = Field(primary_key=True)
    first_name: str = Field(nullable=False)
    last_name: str = Field(nullable=False)
    last_update: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )

    # Relationships
    film_actors: List["FilmActor"] = Relationship(back_populates="actor")