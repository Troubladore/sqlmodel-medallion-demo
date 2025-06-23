from sqlalchemy import MetaData
from sqlmodel import SQLModel

from utils.utils import set_processing_order


@set_processing_order(2)
class TemporalSilverSchema(SQLModel):
    __abstract__ = True
    metadata = MetaData(schema="temporal_silver")