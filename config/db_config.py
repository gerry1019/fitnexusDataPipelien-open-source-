import os

DB_CONFIG = {
    "host": os.getenv("APP_DB_HOST"),
    "port": os.getenv("APP_DB_PORT"),
    "user": os.getenv("APP_DB_USER"),
    "password": os.getenv("APP_DB_PASS"),
    "database": os.getenv("APP_DB_NAME"),
}


def get_jdbc_url():
    return (
        f"jdbc:postgresql://{DB_CONFIG['host']}:"
        f"{DB_CONFIG['port']}/"
        f"{DB_CONFIG['database']}"
        "?sslmode=require"
        "&connectTimeout=60"
        "&socketTimeout=600"
        "&tcpKeepAlive=true"
        "&reWriteBatchedInserts=true"
    )

