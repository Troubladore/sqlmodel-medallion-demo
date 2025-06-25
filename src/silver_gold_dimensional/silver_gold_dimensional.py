"""
🥈🥇 SILVER_GOLD_DIMENSIONAL Database

Purpose: Classic data warehouse patterns
Demonstrates: Traditional dimensional modeling (star schema) in silver/gold layers

This database showcases traditional data warehousing patterns:
- Star schema dimensional modeling (facts + dimensions)
- Silver layer: Clean, business-ready data with quality scores
- Gold layer: Aggregated marts and dimensional models
- Customer/film/rental dimension tables with business keys
- Fact tables with proper foreign key relationships
- Traditional business intelligence preparation
"""

# from engines.postgres_engine import get_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    db_name = "silver_gold_dimensional"  # This can be customized per database script

    # db_url = engine.url.set(database=db_name)  # don't use side-effecting code
    # use make_url to create a mutable copy of engine.url, then set the database name to our desired new name
    db_url = make_url(engine.url).set(database=db_name)

    if not database_exists(db_url):
        create_database(db_url)
        print(f"Database [{db_name}] created successfully.")
    else:
        print(f"Database [{db_name}] already exists.")
