"""
🥉 BRONZE_BASIC Database

Purpose: Foundation bronze implementation
Demonstrates: Basic bronze layer patterns, raw data ingestion with audit fields

This database showcases the fundamental patterns for medallion bronze layer:
- Raw data landing with minimal transformation
- Comprehensive audit fields (load_time, source_file, batch_id, etc.)
- Data quality tracking (is_current, record_hash)
- Lenient typing to handle incoming data quality issues
- Basic bronze table structure and naming conventions
"""

# from engines.postgres_engine import get_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    db_name = "bronze_basic"  # This can be customized per database script

    # db_url = engine.url.set(database=db_name)  # don't use side-effecting code
    # use make_url to create a mutable copy of engine.url, then set the database name to our desired new name
    db_url = make_url(engine.url).set(database=db_name)

    if not database_exists(db_url):
        create_database(db_url)
        print(f"Database [{db_name}] created successfully.")
    else:
        print(f"Database [{db_name}] already exists.")
