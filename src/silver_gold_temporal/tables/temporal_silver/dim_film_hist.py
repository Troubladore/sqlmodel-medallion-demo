from datetime import datetime
from typing import Optional
from enum import Enum

from sqlalchemy import TIMESTAMP, SMALLINT, Numeric, Column, text, Enum as SQLEnum
from sqlmodel import Field, SQLModel


class MPAARating(str, Enum):
    G = "G"
    PG = "PG"
    PG_13 = "PG-13"
    R = "R"
    NC_17 = "NC-17"


class DimFilmHist(SQLModel, table=True):
    """
    System-versioned temporal dimension for films.
    
    Tracks changes to film metadata over time with automatic versioning.
    
    Time-travel examples:
        -- What was the rental rate for film 100 in 2023?
        SELECT title, rental_rate FROM dim_film_hist
        FOR SYSTEM_TIME AS OF '2023-01-01'::timestamptz
        WHERE film_id = 100;
        
        -- Show rental rate changes over time
        SELECT film_id, title, rental_rate, sys_start, sys_end
        FROM dim_film_hist 
        WHERE film_id = 100
        ORDER BY sys_start;
    """
    __tablename__ = "dim_film_hist"
    __table_args__ = {
        "schema": "temporal_silver"
        # Note: PostgreSQL 15+ system versioning would be configured via custom triggers
        # "postgresql_with": "system_versioning = true"  # Not supported in current PostgreSQL
    }

    # Business natural key
    film_id: int = Field(primary_key=True)
    
    # Film attributes
    title: str = Field(nullable=False, max_length=255)
    description: Optional[str] = Field(default=None, nullable=True)
    release_year: Optional[int] = Field(default=None, nullable=True)
    language_id: int = Field(nullable=False)
    original_language_id: Optional[int] = Field(default=None, nullable=True)
    
    # Rental attributes
    rental_duration: int = Field(sa_column=Column(SMALLINT, nullable=False, default=3))
    rental_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False, default=4.99))
    length: Optional[int] = Field(sa_column=Column(SMALLINT, nullable=True), default=None)
    replacement_cost: float = Field(sa_column=Column(Numeric(5, 2), nullable=False, default=19.99))
    rating: Optional[MPAARating] = Field(
        sa_column=Column(SQLEnum(MPAARating, name="mpaa_rating"), nullable=True, default=MPAARating.G)
    )
    
    # System-time versioning columns
    sys_start: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            primary_key=True,
            nullable=False,
            default=text("clock_timestamp()"),
            server_default=text("clock_timestamp()")
        )
    )
    sys_end: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("'infinity'"),
            server_default=text("'infinity'")
        )
    )