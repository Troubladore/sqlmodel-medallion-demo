from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, SMALLINT, Numeric, Column, text
from sqlmodel import Field, SQLModel


class GlDimFilm(SQLModel, table=True):
    __tablename__ = "gl_dim_film"
    __table_args__ = {"schema": "pagila_gold"}

    # Gold surrogate key
    gl_dim_film_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Business natural key
    film_id: int = Field(nullable=False, unique=True)
    
    # Film attributes
    title: str = Field(nullable=False, max_length=255)
    description: Optional[str] = Field(default=None, nullable=True)
    release_year: Optional[int] = Field(default=None, nullable=True)
    rating: Optional[str] = Field(default=None, nullable=True, max_length=10)
    
    # Language attributes (denormalized)
    language_name: str = Field(nullable=False, max_length=20)
    original_language_name: Optional[str] = Field(default=None, nullable=True, max_length=20)
    
    # Categories (denormalized as comma-separated)
    category_names: Optional[str] = Field(default=None, nullable=True, max_length=200)
    primary_category: Optional[str] = Field(default=None, nullable=True, max_length=25)
    
    # Actors (denormalized as comma-separated)
    actor_names: Optional[str] = Field(default=None, nullable=True, max_length=1000)
    lead_actor: Optional[str] = Field(default=None, nullable=True, max_length=90)
    
    # Film specifications
    length_minutes: Optional[int] = Field(sa_column=Column(SMALLINT, nullable=True))
    rental_duration_days: int = Field(sa_column=Column(SMALLINT, nullable=False))
    rental_rate: float = Field(sa_column=Column(Numeric(4, 2), nullable=False))
    replacement_cost: float = Field(sa_column=Column(Numeric(5, 2), nullable=False))
    
    # Special features (denormalized)
    has_trailers: bool = Field(default=False, nullable=False)
    has_commentaries: bool = Field(default=False, nullable=False)
    has_deleted_scenes: bool = Field(default=False, nullable=False)
    has_behind_scenes: bool = Field(default=False, nullable=False)
    special_features_count: int = Field(default=0, nullable=False)
    
    # Film analytics (pre-calculated)
    total_inventory_copies: int = Field(default=0, nullable=False)
    total_rentals: int = Field(default=0, nullable=False)
    total_revenue: float = Field(sa_column=Column(Numeric(8, 2), default=0.00, nullable=False))
    avg_rental_duration: Optional[float] = Field(default=None, nullable=True)
    rental_frequency: Optional[float] = Field(default=None, nullable=True)  # Rentals per day
    
    # Film classification
    length_category: Optional[str] = Field(default=None, nullable=True, max_length=20)  # Short, Medium, Long
    price_tier: Optional[str] = Field(default=None, nullable=True, max_length=20)  # Budget, Standard, Premium
    popularity_score: Optional[float] = Field(default=None, nullable=True)
    is_classic: bool = Field(default=False, nullable=False)  # Based on age and popularity
    
    # Availability
    is_currently_available: bool = Field(default=True, nullable=False)
    last_rental_date: Optional[datetime] = Field(sa_column=Column(TIMESTAMP(timezone=True), nullable=True))
    days_since_last_rental: Optional[int] = Field(default=None, nullable=True)
    
    # Gold audit fields
    gl_created_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    gl_updated_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()"))) 
    gl_source_silver_key: Optional[UUID] = Field(default=None, nullable=True)
    gl_is_current: bool = Field(default=True, nullable=False)