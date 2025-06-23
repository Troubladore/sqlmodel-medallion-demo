from datetime import datetime

from sqlalchemy.dialects.postgresql import TSRANGE
from sqlalchemy.types import TypeDecorator


# idea from https://github.com/fastapi/sqlmodel/issues/235
class TSTupleRange(TypeDecorator):
    impl = TSRANGE
    cache_ok = True  # Add this line to remove the cache warning

    def process_bind_param(self, value, dialect):
        if value is not None:
            # Check if value is a tuple, if not assume it's a single datetime
            if isinstance(value, tuple):
                return f"[{value[0]},{value[1]})"
            elif isinstance(value, datetime):
                # Handle the case where only a single datetime is provided
                return f"[{value},)"
            else:
                raise ValueError(
                    "Expected a tuple of (start_time, end_time) or a single datetime object"
                )
        return None

    def process_result_value(self, value, dialect):
        if value is not None:
            # Assuming value is a Range object provided by SQLAlchemy
            start = value.lower
            end = value.upper
            return start, end
        return None
