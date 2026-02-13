from sqlalchemy import text


def get_watermark(key: str, conn):
    query = text("""
                 SELECT last_ingested_at
                 FROM ingestion_watermark
                 WHERE table_name = :key
                 """)

    # Check if we are in 'future' mode or legacy
    try:
        result = conn.execute(query, {"key": key}).scalar()
    except:
        # Fallback for some legacy setups
        result = conn.execute(query, key=key).scalar()

    return result


def set_watermark(key: str, new_wm, conn):
    query = text("""
                 INSERT INTO ingestion_watermark (table_name, last_ingested_at)
                 VALUES (:key, :wm) ON CONFLICT (table_name)
        DO
                 UPDATE SET last_ingested_at = EXCLUDED.last_ingested_at,
                     updated_at = NOW();
                 """)

    # ✅ FIX: Using 'begin()' handles the commit automatically
    # This works on BOTH legacy and future SQLAlchemy versions
    with conn.begin():
        conn.execute(query, {"key": key, "wm": new_wm})
        print(f"🔄 Watermark saved for {key}")