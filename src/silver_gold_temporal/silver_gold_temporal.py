"""
⏰ SILVER_GOLD_TEMPORAL Database

Purpose: Time-variant dimensional modeling
Demonstrates: Temporal/slowly changing dimensions with history tracking

This database showcases advanced temporal patterns for medallion architecture:
- Slowly Changing Dimensions (SCD) with full history
- Temporal silver layer: sys_start/sys_end time ranges
- History tables with temporal validity periods
- Point-in-time dimensional analysis capabilities
- Current + historical views of dimensional data
- Time-travel queries and historical reporting
"""

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    db_name = "silver_gold_temporal"

    db_url = make_url(engine.url).set(database=db_name)

    if not database_exists(db_url):
        create_database(db_url)
        print(f"Database [{db_name}] created successfully.")
    else:
        print(f"Database [{db_name}] already exists.")