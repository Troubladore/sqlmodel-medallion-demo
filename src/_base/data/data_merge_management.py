from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Generic, List, Type, TypeVar

from sqlmodel import Session, SQLModel, select

T = TypeVar("T", bound=SQLModel)


class ReferenceData(ABC, Generic[T]):
    """Abstract base class for reference data definitions"""

    @abstractmethod
    def get_records(self) -> List[T]:
        """Return list of reference data records"""

    @abstractmethod
    def get_id_field(self) -> str:
        """Return the name of the ID field for this reference data type"""

    @abstractmethod
    def get_comparable_fields(self) -> List[str]:
        """Return list of field names to compare when checking for changes"""

    @abstractmethod
    def get_model_class(self) -> Type[T]:
        """Return the model class for this reference data"""


class ReferenceDataBase(ReferenceData[T]):
    """Base implementation of reference data handler"""

    def __init__(
        self,
        model_class: Type[T],
        id_field: str,
        comparable_fields: List[str],
        records: List[T],
    ):
        self._model_class = model_class
        self._id_field = id_field
        self._comparable_fields = comparable_fields
        self._records = records

    def get_records(self) -> List[T]:
        return self._records

    def get_id_field(self) -> str:
        return self._id_field

    def get_comparable_fields(self) -> List[str]:
        return self._comparable_fields

    def get_model_class(self) -> Type[T]:
        return self._model_class


class ReferenceDataFactory:
    """Factory for creating and managing reference data handlers"""

    @staticmethod
    def create_handler(
        model_class: Type[T],
        id_field: str,
        comparable_fields: List[str],
        records: List[T],
    ) -> ReferenceData[T]:
        """Create a reference data handler for a specific model"""
        return ConcreteReferenceData(model_class, id_field, comparable_fields, records)


class ConcreteReferenceData(ReferenceData[T]):
    """Concrete implementation of reference data handler"""

    def __init__(
        self,
        model_class: Type[T],
        id_field: str,
        comparable_fields: List[str],
        records: List[T],
    ):
        self._model_class = model_class
        self._id_field = id_field
        self._comparable_fields = comparable_fields
        self._records = records

    def get_records(self) -> List[T]:
        return self._records

    def get_id_field(self) -> str:
        return self._id_field

    def get_comparable_fields(self) -> List[str]:
        return self._comparable_fields


class ReferenceDataManager:
    """Manager class for handling reference data operations"""

    def __init__(self, session: Session):
        self.session = session

    def merge_reference_data(self, handler: ReferenceData[T]) -> None:
        """
        Implements a MERGE-like pattern for reference data:
        - Updates existing records if they differ
        - Inserts new records
        - Optionally deactivates records not in the new set
        """
        # Get all existing records
        existing_records = self.session.exec(select(handler.get_model_class())).all()

        # Create lookup dictionaries
        id_field = handler.get_id_field()
        existing_by_id = {
            getattr(record, id_field): record for record in existing_records
        }
        new_records = handler.get_records()
        new_by_id = {getattr(record, id_field): record for record in new_records}

        # Update existing records that differ
        for record_id, new_record in new_by_id.items():
            if record_id in existing_by_id:
                existing_record = existing_by_id[record_id]
                if self._records_differ(
                    existing_record, new_record, handler.get_comparable_fields()
                ):
                    self._update_record(
                        existing_record, new_record, handler.get_comparable_fields()
                    )
                    self.session.add(existing_record)

        # Insert new records
        for record_id, new_record in new_by_id.items():
            if record_id not in existing_by_id:
                self.session.add(new_record)

        # Deactivate records not in new set
        for record_id, existing_record in existing_by_id.items():
            if record_id not in new_by_id and hasattr(existing_record, "inactivated_date"):
                existing_record.inactivated_date = datetime.now(UTC)
                self.session.add(existing_record)

    @staticmethod
    def _records_differ(existing: T, new: T, comparable_fields: List[str]) -> bool:
        """Compare relevant fields to determine if records are different"""
        return any(
            getattr(existing, field) != getattr(new, field)
            for field in comparable_fields
        )

    @staticmethod
    def _update_record(existing: T, new: T, comparable_fields: List[str]) -> None:
        """Update mutable fields of existing record with values from new record"""
        for field in comparable_fields:
            setattr(existing, field, getattr(new, field))

        if hasattr(existing, "systime"):
            existing.systime = datetime.now(UTC)