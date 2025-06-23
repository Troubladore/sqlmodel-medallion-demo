from datetime import datetime, date
from typing import Optional

from sqlalchemy import TIMESTAMP, Date, Column, text
from sqlmodel import Field, SQLModel


class GlDimDate(SQLModel, table=True):
    __tablename__ = "gl_dim_date"
    __table_args__ = {"schema": "pagila_gold"}

    # Date key (YYYYMMDD format as integer)
    date_key: int = Field(primary_key=True)  # e.g., 20241220 for Dec 20, 2024
    
    # Full date
    full_date: date = Field(sa_column=Column(Date, nullable=False, unique=True))
    
    # Date components
    day_of_month: int = Field(nullable=False)  # 1-31
    day_of_year: int = Field(nullable=False)   # 1-366
    day_of_week: int = Field(nullable=False)   # 1-7 (Monday=1)
    day_name: str = Field(nullable=False, max_length=9)  # Monday, Tuesday, etc.
    day_name_short: str = Field(nullable=False, max_length=3)  # Mon, Tue, etc.
    
    # Week information
    week_of_year: int = Field(nullable=False)  # 1-53
    week_of_month: int = Field(nullable=False) # 1-6
    weekday_indicator: str = Field(nullable=False, max_length=7)  # Weekday/Weekend
    
    # Month information
    month_number: int = Field(nullable=False)  # 1-12
    month_name: str = Field(nullable=False, max_length=9)  # January, February, etc.
    month_name_short: str = Field(nullable=False, max_length=3)  # Jan, Feb, etc.
    month_year: str = Field(nullable=False, max_length=7)  # 2024-12
    
    # Quarter information
    quarter_number: int = Field(nullable=False)  # 1-4
    quarter_name: str = Field(nullable=False, max_length=2)  # Q1, Q2, Q3, Q4
    quarter_year: str = Field(nullable=False, max_length=7)  # 2024-Q4
    
    # Year information
    year_number: int = Field(nullable=False)  # 2024
    
    # Business flags
    is_weekend: bool = Field(default=False, nullable=False)
    is_weekday: bool = Field(default=True, nullable=False)
    is_holiday: bool = Field(default=False, nullable=False)
    is_business_day: bool = Field(default=True, nullable=False)
    
    # Holiday information
    holiday_name: Optional[str] = Field(default=None, nullable=True, max_length=50)
    holiday_type: Optional[str] = Field(default=None, nullable=True, max_length=20)  # Federal, State, Religious, etc.
    
    # Fiscal year (assuming April-March fiscal year, can be adjusted)
    fiscal_year: int = Field(nullable=False)
    fiscal_quarter: int = Field(nullable=False)  # 1-4
    fiscal_month: int = Field(nullable=False)   # 1-12
    
    # Relative date flags (useful for analytics)
    is_current_day: bool = Field(default=False, nullable=False)
    is_current_week: bool = Field(default=False, nullable=False)
    is_current_month: bool = Field(default=False, nullable=False)
    is_current_quarter: bool = Field(default=False, nullable=False)
    is_current_year: bool = Field(default=False, nullable=False)
    
    # Days from reference points
    days_from_today: Optional[int] = Field(default=None, nullable=True)
    
    # Special business periods
    is_month_end: bool = Field(default=False, nullable=False)
    is_quarter_end: bool = Field(default=False, nullable=False)
    is_year_end: bool = Field(default=False, nullable=False)
    is_leap_year: bool = Field(default=False, nullable=False)
    
    # Season information
    season: str = Field(nullable=False, max_length=6)  # Spring, Summer, Fall, Winter
    
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