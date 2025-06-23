import os

from sqlalchemy.engine import Engine
from sqlmodel import create_engine

from config import DEFAULT_DATABASE_NAME


def get_sqlite_file_path(db_name: str) -> str:
    sqlite_db_name = f"sqlite_{db_name}.db"

    # Get the absolute path of the parent directory
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Construct the path to the .data directory in the parent folder
    data_dir = os.path.join(parent_dir, ".data")

    # Create the .data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)

    # Construct the SQLite file path
    sqlite_file_path = os.path.join(data_dir, sqlite_db_name)

    return sqlite_file_path


def get_connection_url(db_name: str, in_memory: bool) -> str:
    if in_memory:
        return "sqlite://"
    else:
        sqlite_file_path = get_sqlite_file_path(db_name)
        return f"sqlite:///{sqlite_file_path}"


def get_engine(
    echo_msgs: bool = True,
    db_name: str = DEFAULT_DATABASE_NAME,
    in_memory: bool = False,
) -> Engine:
    connection_url = get_connection_url(db_name, in_memory)
    engine = create_engine(connection_url, echo=echo_msgs)
    return engine
