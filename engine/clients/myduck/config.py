import os

MYDUCK_VECTOR_PORT = int(os.getenv("MYDUCK_VECTOR_PORT", 5432))
MYDUCK_VECTOR_DB = os.getenv("MYDUCK_VECTOR_DB", "postgres")
MYDUCK_VECTOR_USER = os.getenv("MYDUCK_VECTOR_USER", "postgres")
MYDUCK_VECTOR_PASSWORD = os.getenv("MYDUCK_VECTOR_PASSWORD", "passwd")


def get_db_config(host, connection_params):
    return {
        "host": host or "localhost",
        "port": MYDUCK_VECTOR_PORT,
        "dbname": MYDUCK_VECTOR_DB,
        "user": MYDUCK_VECTOR_USER,
        "password": MYDUCK_VECTOR_PASSWORD,
        "autocommit": True,
        **connection_params,
    }
