from datetime import datetime
from typing import TYPE_CHECKING, Optional

from sqlalchemy import TIMESTAMP, Column, ForeignKey, text
from sqlmodel import Field, Relationship, SQLModel

if TYPE_CHECKING:
    from src.pagila.tables.pagila.actor import Actor
    from src.pagila.tables.pagila.film import Film


class FilmActor(SQLModel, table=True):
    __tablename__ = "film_actor"
    __table_args__ = {"schema": "public"}

    actor_id: int = Field(
        sa_column=Column(ForeignKey("public.actor.actor_id"), primary_key=True)
    )
    film_id: int = Field(
        sa_column=Column(ForeignKey("public.film.film_id"), primary_key=True)
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
    actor: Optional["Actor"] = Relationship(back_populates="film_actors")
    film: Optional["Film"] = Relationship(back_populates="film_actors")