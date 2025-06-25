from datetime import UTC, datetime
from typing import List, Type

from sqlalchemy.sql.selectable import Select
from sqlmodel import Session, SQLModel, select


def merge_records(
    session: Session, model: Type[SQLModel], records: List[dict], strict: bool = True
) -> None:
    """
    Merge records into the database, updating existing ones and inserting new ones.

    Args:
        session: SQLModel session
        model: The SQLModel model class
        records: List of dictionaries containing record data
        strict: If True, removes records not in the input set. If False, only updates/adds records.
    """
    # Filter to only active records (inactivated_date = None means active)
    active_records = [r for r in records if r.get("inactivated_date") is None]

    # Get primary key field using SQLModel v2 approach
    pk_field = None
    for field_name, field in model.model_fields.items():
        if (
            field.annotation == int
            and field.json_schema_extra
            and field.json_schema_extra.get("sa_column_kwargs", {}).get("primary_key")
        ):
            pk_field = field_name
            break

    if not pk_field:
        # Fallback to check Field properties directly
        for field_name, field in model.model_fields.items():
            if hasattr(field, "primary_key") and field.primary_key:
                pk_field = field_name
                break

    if not pk_field:
        raise ValueError(f"No primary key field found for model {model.__name__}")

    # Get existing records
    stmt: Select = select(model)
    existing_records = session.exec(stmt).all()
    existing_ids = {getattr(record, pk_field) for record in existing_records}

    # Add or update records
    for record in active_records:
        # Add systime if it exists in the model
        if hasattr(model, "systime"):
            record["systime"] = datetime.now(UTC)

        record_id = record[pk_field]

        if record_id in existing_ids:
            # Update existing record
            stmt = select(model).where(getattr(model, pk_field) == record_id)
            db_record = session.exec(stmt).one()
            for key, value in record.items():
                setattr(db_record, key, value)
        else:
            # Add new record
            db_record = model(**record)
            session.add(db_record)

    if strict:
        # Remove records not in the input set
        input_ids = {r[pk_field] for r in active_records}
        for existing_id in existing_ids - input_ids:
            stmt = select(model).where(getattr(model, pk_field) == existing_id)
            record_to_delete = session.exec(stmt).one()
            session.delete(record_to_delete)

    session.commit()