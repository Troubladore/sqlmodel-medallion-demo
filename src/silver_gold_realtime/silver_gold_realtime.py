"""
⚡ SILVER_GOLD_REALTIME Database

Purpose: Streaming/real-time analytics patterns
Demonstrates: Real-time analytics and operational dashboards

This database showcases real-time analytics patterns for medallion architecture:
- Real-time KPI calculation and monitoring
- Operational silver layer: live business metrics
- Gold layer: Real-time dashboards and executive summaries
- Alert management and escalation workflows
- Live business intelligence and monitoring
- Streaming analytics and operational insights
"""

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    """Create the pagila_ops_silver_gold database for operational analytics."""
    db_name = "silver_gold_realtime"
    db_url = make_url(engine.url).set(database=db_name)
    
    if not database_exists(db_url):
        create_database(db_url)
        print(f"✅ Created database: {db_name}")
    else:
        print(f"ℹ️  Database already exists: {db_name}")