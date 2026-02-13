CREATE TABLE IF NOT EXISTS ingestion_watermark (
    table_name TEXT PRIMARY KEY,
    last_ingested_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NOW()
);
