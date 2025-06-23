from sqlalchemy import MetaData
from sqlmodel import SQLModel
from utils.utils import set_processing_order


@set_processing_order(3)
class OpsGoldSchema(SQLModel):
    """Schema for operational dashboards and live monitoring."""
    __abstract__ = True
    metadata = MetaData(schema="ops_gold")