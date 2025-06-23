from sqlalchemy import MetaData
from sqlmodel import SQLModel

from utils.utils import set_processing_order


# Create a custom SQLModel class for the schema
@set_processing_order(0)  # process before any other schemas
class CommonSchema(SQLModel):
    __abstract__ = True
    metadata = MetaData(schema="common")
