"""
🔄 BRONZE_CDC Database

Purpose: CDC-based bronze ingestion
Demonstrates: Change Data Capture patterns in bronze layer

This database showcases advanced bronze layer patterns for change tracking:
- CDC (Change Data Capture) event processing
- Delta/incremental data ingestion patterns
- Change event metadata and tracking
- Streaming data pipeline foundations
- Event-driven bronze layer architecture
"""

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    db_name = "bronze_cdc"

    db_url = make_url(engine.url).set(database=db_name)

    if not database_exists(db_url):
        create_database(db_url)
        print(f"Database [{db_name}] created successfully.")
    else:
        print(f"Database [{db_name}] already exists.")