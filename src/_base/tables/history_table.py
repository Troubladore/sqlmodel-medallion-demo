import uuid
from typing import Optional
from uuid import UUID

from sqlmodel import Field, SQLModel

from src._base.columns.ts_tuple_range import TSTupleRange


class HistoryTable(SQLModel):
    __abstract__ = True
    # Allow arbitrary types to be used in the model without schema generation issues
    model_config = {"arbitrary_types_allowed": True}

    history_id: UUID = Field(default_factory=uuid.uuid4, primary_key=True)
    effective_time: TSTupleRange
    systime: TSTupleRange
    operation_type: Optional[bool] = Field(default=None)

    # All this logic has been moved to the indivudual history tables. Only one
    # history table could inherit effective_time and caused errors
    """# Prepare the default, standard fields for all history tables
    @declared_attr
    def history_id(cls) -> UUID:
        return Field(default_factory=uuid.uuid4, primary_key=True)
    
    @declared_attr
    def effective_time(cls) -> TSTupleRange:
        return Field(sa_column=Column("effective_time", TSTupleRange, nullable=False))
    
    @declared_attr
    def systime(cls) -> TSTupleRange:
        return Field(sa_column=Column("systime", TSTupleRange, nullable=False))

    # operation_type column as nullable boolean (1: Insert, 0: Update, NULL: Delete)
    @declared_attr
    def operation_type(cls) -> Optional[bool]:
        return Field(sa_column=Column("operation_type", Boolean, nullable=True))"""

    '''
    @classmethod
    def __init_subclass__(cls, **kwargs):
        """Dynamically add constraints and indexes for each subclass."""
        super().__init_subclass__(**kwargs)

        # Ensure __tablename__ is defined in the subclass
        if hasattr(cls, "__tablename__"):
            # Dynamically create constraint names based on the table name
            effective_time_check_name = f"{cls.__tablename__}_effective_time_check"
            systime_check_name = f"{cls.__tablename__}_systime_check"

            # Dynamically create index names based on the table name
            effective_time_index_name = f"idx_{cls.__tablename__}_effective_time_gist"
            systime_index_name = f"idx_{cls.__tablename__}_systime_gist"

            # Apply constraints and indexes to the class
            cls.__table_args__ = (
                CheckConstraint(
                    "NOT isempty(effective_time)", name=effective_time_check_name
                ),
                CheckConstraint("NOT isempty(systime)", name=systime_check_name),
                Index(
                    effective_time_index_name, "effective_time", postgresql_using="gist"
                ),
                Index(systime_index_name, "systime", postgresql_using="gist"),
            )
'''
