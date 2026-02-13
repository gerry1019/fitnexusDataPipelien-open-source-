from utils.logger import get_logger
from utils.watermark_store import get_watermark, set_watermark
from utils.db_utils import get_postgres_engine
from utils.s3_utils import upload_parquet_to_s3

logger = get_logger("bronze_pipeline")


def run_postgres_to_s3_bronze(table_name, pg_conn, s3_bucket, s3_prefix):
    """
    Incremental ingestion of a PostgreSQL table into MinIO (S3) in Parquet format.
    Uses ingestion_watermark table to track the last ingested timestamp.
    """

    # Build watermark key (same as table name)
    watermark_key = table_name

    # Read watermark properly (REQUIRES pg_conn)
    last_wm = get_watermark(pg_conn, watermark_key)
    logger.info(f"Last watermark for {table_name}: {last_wm}")

    # Build incremental query
    if last_wm:
        query = f"""
            SELECT * FROM {table_name}
            WHERE updated_at > '{last_wm}'
            ORDER BY updated_at ASC;
        """
    else:
        # Full load on first run
        query = f"SELECT * FROM {table_name};"

    logger.info(f"Running query: {query}")

    # Load data
    df = pg_conn.execute(query).fetchall()
    df = df.to_dataframe() if hasattr(df, "to_dataframe") else df

    if df is None or len(df) == 0:
        logger.info(f"No new data found for {table_name}. Skipping.")
        return

    # Convert SQLAlchemy rows to DataFrame if needed
    if not hasattr(df, "columns"):
        import pandas as pd
        df = pd.DataFrame(df, columns=df[0].keys() if len(df) > 0 else [])

    # Determine new watermark
    max_updated_at = df["updated_at"].max()
    logger.info(f"New watermark for {table_name}: {max_updated_at}")

    # Upload to S3
    key = f"{s3_prefix}/{table_name}/{table_name}_{max_updated_at}.parquet"
    upload_parquet_to_s3(df, s3_bucket, key)

    # Update watermark (pass pg_conn!)
    set_watermark(pg_conn, watermark_key, max_updated_at)
    logger.info(f"Watermark updated for {table_name}")
