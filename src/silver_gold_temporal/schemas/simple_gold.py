from sqlalchemy import MetaData
from sqlmodel import SQLModel

from utils.utils import set_processing_order


@set_processing_order(3)
class SimpleGoldSchema(SQLModel):
    __abstract__ = True
    metadata = MetaData(schema="simple_gold")