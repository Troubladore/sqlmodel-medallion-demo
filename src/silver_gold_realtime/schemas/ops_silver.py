from sqlalchemy import MetaData
from sqlmodel import SQLModel
from utils.utils import set_processing_order


@set_processing_order(2)
class OpsSilverSchema(SQLModel):
    """Schema for real-time operational aggregations and alerting."""
    __abstract__ = True
    metadata = MetaData(schema="ops_silver")