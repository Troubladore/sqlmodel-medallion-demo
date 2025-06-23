from sqlalchemy import MetaData
from sqlmodel import SQLModel
from utils.utils import set_processing_order


@set_processing_order(1)
class OpsBronzeSchema(SQLModel):
    """Schema for operational event streaming and real-time data capture."""
    __abstract__ = True
    metadata = MetaData(schema="ops_bronze")