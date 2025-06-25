"""
📡 BRONZE_STREAMING Database

Purpose: Streaming/real-time analytics patterns
Demonstrates: Operational monitoring and real-time event streaming in bronze layer

This database showcases streaming and operational patterns for medallion bronze:
- High-velocity operational event ingestion
- Real-time system metrics and alerts
- Business event streaming (rental_start, payment_processed, etc.)
- Operational telemetry and monitoring data
- Event-driven architecture foundations
- Streaming analytics preparation layer
"""

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    """Create the pagila_ops_bronze database for operational event streaming."""
    db_name = "bronze_streaming"
    db_url = make_url(engine.url).set(database=db_name)
    
    if not database_exists(db_url):
        create_database(db_url)
        print(f"✅ Created database: {db_name}")
    else:
        print(f"ℹ️  Database already exists: {db_name}")