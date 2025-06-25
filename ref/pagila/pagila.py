from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy_utils import create_database, database_exists


def create_target_db(engine: Engine) -> None:
    db_name = "pagila"

    db_url = make_url(engine.url).set(database=db_name)

    if not database_exists(db_url):
        create_database(db_url)
        print(f"Database [{db_name}] created successfully.")
    else:
        print(f"Database [{db_name}] already exists.")