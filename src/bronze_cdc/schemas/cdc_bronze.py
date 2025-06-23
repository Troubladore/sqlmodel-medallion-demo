from sqlalchemy import MetaData
from sqlmodel import SQLModel

from utils.utils import set_processing_order


@set_processing_order(1)
class CdcBronzeSchema(SQLModel):
    __abstract__ = True
    metadata = MetaData(schema="cdc_bronze")