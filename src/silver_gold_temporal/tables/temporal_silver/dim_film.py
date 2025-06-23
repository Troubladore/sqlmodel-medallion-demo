from datetime import datetime
from typing import Optional
from enum import Enum

from sqlalchemy import SMALLINT, Numeric, Column, Enum as SQLEnum
from sqlmodel import Field, SQLModel


class MPAARating(str, Enum):
    G = "G"
    PG = "PG"
    PG_13 = "PG-13"
    R = "R"
    NC_17 = "NC-17"


class DimFilm(SQLModel, table=False):
    """
    Current-state view of film dimension.
    
    Hides temporal complexity and only shows current film data.
    
    SQL Definition:
        CREATE OR REPLACE VIEW temporal_silver.dim_film AS
        SELECT film_id, title, description, release_year, language_id, 
               original_language_id, rental_duration, rental_rate, length,
               replacement_cost, rating, sys_start
        FROM temporal_silver.dim_film_hist
        WHERE sys_end = 'infinity'::timestamptz;
    
    Usage:
        -- Current film catalog
        SELECT title, rental_rate FROM dim_film 
        WHERE rating = 'G' ORDER BY title;
        
        -- Historical pricing analysis
        SELECT film_id, title, rental_rate, sys_start, sys_end
        FROM dim_film_hist 
        WHERE film_id = 100 ORDER BY sys_start;
    """
    __tablename__ = "dim_film"
    __table_args__ = {"schema": "temporal_silver"}

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
    
    # Current version timestamp
    sys_start: datetime = Field(nullable=False)