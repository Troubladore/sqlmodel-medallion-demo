from sqlalchemy import MetaData
from sqlmodel import SQLModel

from utils.utils import set_processing_order


# Create a custom SQLModel class for the schema
@set_processing_order(2)  # process before any other schemas
class PagilaSilverSchema(SQLModel):
    __abstract__ = True
    metadata = MetaData(schema="pagila_silver")
