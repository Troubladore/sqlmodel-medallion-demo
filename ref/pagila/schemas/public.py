from sqlalchemy import MetaData
from sqlmodel import SQLModel

from utils.utils import set_processing_order


@set_processing_order(0)
class PagilaPublicSchema(SQLModel):
    __abstract__ = True
    metadata = MetaData(schema="public")