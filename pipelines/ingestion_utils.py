import pandas as pd
from sqlalchemy import text
from utils.db_utils import get_postgres_engine

# -----------------------------------
# Watermark table functions
# -----------------------------------

def get_watermark(table_name: str):
    """
    Fetch last_ingested_at watermark for the given table.
    If not found, return None.
    """
    engine = get_postgres_engine()
    query = text("""
        SELECT last_ingested_at 
        FROM ingestion_watermark
        WHERE table_name = :table_name
        LIMIT 1
    """)
    with engine.connect() as conn:
        result = conn.execute(query, {"table_name": table_name}).fetchone()
        return result[0] if result else None


def update_watermark(table_name: str, timestamp_value):
    """
    Update the watermark after successful ingestion.
    If row does not exist → INSERT
    If exists → UPDATE
    """
    engine = get_postgres_engine()
    query = text("""
        INSERT INTO ingestion_watermark (table_name, last_ingested_at)
        VALUES (:table_name, :ts)
        ON CONFLICT (table_name)
        DO UPDATE SET last_ingested_at = EXCLUDED.last_ingested_at,
                      updated_at = NOW();
    """)

    with engine.connect() as conn:
        conn.execute(query, {"table_name": table_name, "ts": timestamp_value})
        conn.commit()

    print(f"🔄 Watermark updated for {table_name} → {timestamp_value}")


# -----------------------------------
# Backfill logic
# -----------------------------------

def run_backfill(table_name: str, start_date: str, end_date: str):
    """
    Runs a backfill load by extracting data between date ranges
    """
    engine = get_postgres_engine()

    # Your tables must have createdAt or updatedAt
    query = text(f"""
        SELECT *
        FROM {table_name}
        WHERE createdAt >= :start_date
        AND createdAt <= :end_date
    """)

    print(f"🏗 Running backfill for {table_name}: {start_date} → {end_date}")

    df = pd.read_sql(query, engine, params={
        "start_date": start_date,
        "end_date": end_date
    })

    print(f"📦 Extracted {len(df)} rows from {table_name}")

    return df
