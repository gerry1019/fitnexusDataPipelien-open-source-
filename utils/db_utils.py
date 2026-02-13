from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from config.db_config import POSTGRES_APP_CONFIG
import urllib.parse


def get_postgres_engine():
    # 1. URL Encode password to be safe
    encoded_password = urllib.parse.quote_plus(POSTGRES_APP_CONFIG['password'])

    # 2. Construct the URI for SQLAlchemy
    conn_str = (
        f"postgresql+psycopg2://{POSTGRES_APP_CONFIG['user']}:"
        f"{encoded_password}@"
        f"{POSTGRES_APP_CONFIG['host']}:"
        f"{POSTGRES_APP_CONFIG['port']}/"
        f"{POSTGRES_APP_CONFIG['dbname']}"
        f"?sslmode=require"
    )

    # 3. Create Engine with 'future=True'
    # ✅ FIX: future=True adds the .commit() method to connections
    return create_engine(
        conn_str,
        poolclass=NullPool,
        echo=False,
        pool_pre_ping=True,
        future=True  # <--- CRITICAL FIX
    )