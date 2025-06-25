import getpass
import os
import platform
import subprocess
from typing import Optional

from sqlalchemy.engine import Engine
from sqlmodel import create_engine

# NOTE - under the covers, SQLAlchemy will require that you've poetry added psycopg2-binary to the local .venv
#        in order to connect to Postgres


def get_connection_url(database_name, conn_method, user_name, host="localhost", port=5432) -> str:
    if conn_method == "password":
        # defined in /etc/postgresql/<version>/main/postgresql.conf as...
        # host    all    myuser    127.0.0.1/32    scram-sha-256
        postgres_url = (
            f"postgresql://{user_name}:password@{host}:{port}/{database_name}"
        )
    elif conn_method == "tcp":
        # this is for trusted TCPIP connections
        # defined in /etc/postgresql/<version>/main/postgresql.conf as...
        # host    all    myuser    127.0.0.1/32    trust
        postgres_url = f"postgresql://{user_name}@{host}:{port}/{database_name}?gssencmode=disable"
    elif conn_method == "socket":
        # this is for trusted domain socket connections (fastest connections)
        # defined in /etc/postgresql/<version>/main/postgresql.conf as...
        # local    all   myuser    peer
        postgres_url = (
            f"postgresql://{user_name}@/{database_name}?host=/var/run/postgresql"
        )
    else:
        raise Exception("Invalid connection method")

    return postgres_url


def find_postgres_socket_dir(port: int = 5432) -> Optional[str]:
    # Skip socket path detection on Windows - sockets don't work the same way
    if platform.system() == "Windows":
        return None
    
    # First, try to find the socket in the default location
    default_socket_dirs = ["/var/run/postgresql", "/tmp"]
    for dir in default_socket_dirs:
        socket_path = os.path.join(dir, f".s.PGSQL.{port}")
        if os.path.exists(socket_path):
            return dir

    # If not found in default locations, try using ss command
    try:
        result = subprocess.run(
            ["ss", "-x", "state", "listening"],
            capture_output=True,
            text=True,
            check=True,
        )

        for line in result.stdout.splitlines():
            if f"s.PGSQL.{port}" in line:
                # Extract the socket path
                socket_path = line.split()[-1]
                return os.path.dirname(socket_path)

    except subprocess.CalledProcessError:
        pass  # ss command failed, continue to next method

    # If ss doesn't work, try netstat (which might require sudo)
    try:
        result = subprocess.run(
            ["sudo", "netstat", "-lnp"], capture_output=True, text=True, check=True
        )

        for line in result.stdout.splitlines():
            if f":{port}" in line and "LISTENING" in line:
                pid = line.split()[-1].split("/")[0]
                # Get the working directory of the process
                with open(f"/proc/{pid}/cwd") as f:
                    return f.read().strip()

    except subprocess.CalledProcessError:
        pass  # netstat command failed

    # If all methods fail, return None
    return None


def find_postgres_socket_dir_old(port: int = 5432) -> str:
    try:
        # Run the lsof command to list all Unix sockets
        result = subprocess.run(
            ["lsof", "-U"], capture_output=True, text=True, check=True
        )

        # Parse the output to find the line with the PostgreSQL socket
        for line in result.stdout.splitlines():
            if f".s.PGSQL.{port}" in line:
                # Extract the socket path (assuming it's the last word on the line)
                socket_path = line.split()[-1]
                socket_dir = os.path.dirname(socket_path)  # Get the directory part
                return socket_dir
        raise FileNotFoundError(f"No socket found for PostgreSQL on port {port}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to run lsof: {e}")


def get_engine(
    echo_msgs=True,
    database_name="postgres",
    conn_method="socket",
    user_name=None,
    host="localhost",
    port=5432,
) -> Engine:
    if user_name is None:
        user_name = getpass.getuser()  # Fetches the current user independent of OS
    if conn_method == "socket":
        socket_path = find_postgres_socket_dir()
        if socket_path is None:
            # Fallback to TCP connection on Windows or when socket not found
            print("Socket path not found, falling back to TCP connection")
            instance_url = get_connection_url(database_name, "tcp", user_name, host, port)
        else:
            print(socket_path)
            instance_url = f"postgresql+psycopg2://{user_name}@localhost/{database_name}?host={socket_path}"
    else:
        instance_url = get_connection_url(database_name, conn_method, user_name, host, port)

    engine = create_engine(
        instance_url, echo=echo_msgs
    )  # don't echo (execution messages) in prod
    return engine
