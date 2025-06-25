from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import TIMESTAMP, Column, text
from sqlmodel import Field, SQLModel


class BrLanguage(SQLModel, table=True):
    __tablename__ = "br_language"
    __table_args__ = {"schema": "staging_pagila"}

    # Bronze surrogate key
    br_language_key: UUID = Field(default_factory=uuid4, primary_key=True)
    
    # Source data fields (lenient types for data quality issues)
    language_id: Optional[str] = Field(default=None, nullable=True)
    name: Optional[str] = Field(default=None, nullable=True)
    last_update: Optional[str] = Field(default=None, nullable=True)
    
    # Bronze audit fields
    br_load_time: datetime = Field(
        sa_column=Column(
            TIMESTAMP(timezone=True),
            nullable=False,
            default=text("now()"),
            server_default=text("now()")
        )
    )
    br_source_file: Optional[str] = Field(default=None, nullable=True, max_length=500)
    br_record_hash: Optional[str] = Field(default=None, nullable=True, max_length=64)
    br_is_current: bool = Field(default=True, nullable=False)
    br_batch_id: Optional[str] = Field(default=None, nullable=True, max_length=100)