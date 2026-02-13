import pandas as pd
from datetime import datetime
from sqlalchemy import text

from config.pipeline_config import TABLE_LIST
from config.minio_config import MINIO_CONFIG
from utils.db_utils import get_postgres_engine
from utils.watermark_store import get_watermark, set_watermark
from utils.s3_utils import upload_parquet_to_s3


def run_full_ingestion():
    engine = get_postgres_engine()

    for table_name, meta in TABLE_LIST.items():
        print(f"\n🚀 Processing table: {table_name}")

        updated_column = meta["updated_column"]
        full_load_flag = meta.get("full_load", False)
        wm_key = f"wm_{table_name}"

        # ---------------------------------------------------------
        # STEP 1: OPEN CONNECTION -> FETCH DATA -> CLOSE CONNECTION
        # ---------------------------------------------------------
        with engine.connect() as conn:

            # Get watermark
            last_wm = get_watermark(wm_key, conn)
            print(f"➡️ Last watermark: {last_wm}")

            # Prepare Query
            if full_load_flag or last_wm is None:
                print(f"⚠️ FULL LOAD for {table_name}")
                query = text(f'SELECT * FROM "{table_name}"')
                query_params = {}
            else:
                print(f"🚀 INCREMENTAL LOAD for {table_name}")
                query = text(f'''
                    SELECT *
                    FROM "{table_name}"
                    WHERE "{updated_column}" > :wm
                ''')
                query_params = {"wm": last_wm}

            # Fetch Data directly with SQLAlchemy
            try:
                result = conn.execute(query, query_params)
                rows = result.fetchall()

                if not rows:
                    print(f"⛔ No new data for {table_name}")
                    continue

                # Manually construct DataFrame
                df = pd.DataFrame(rows, columns=list(result.keys()))

            except Exception as e:
                print(f"❌ Error reading {table_name}: {e}")
                raise e

            # Calculate new watermark
            new_wm = df[updated_column].max()

        # ---------------------------------------------------------
        # STEP 2: UPLOAD TO S3
        # ---------------------------------------------------------
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"bronze/{table_name}/{table_name}_{timestamp}.parquet"

        try:
            upload_parquet_to_s3(df, MINIO_CONFIG["bucket"], s3_key)
            print(f"🪣 Uploaded → s3://{MINIO_CONFIG['bucket']}/{s3_key}")
        except Exception as e:
            print(f"❌ Failed to upload to S3: {e}")
            raise e

        # ---------------------------------------------------------
        # STEP 3: RE-OPEN CONNECTION -> UPDATE WATERMARK
        # ---------------------------------------------------------
        with engine.connect() as conn:
            # ✅ FIX: Check if watermark is valid (Not NaN/NaT) before saving
            if pd.isna(new_wm):
                print(
                    f"⚠️ Warning: Max watermark for {table_name} is NaT/NaN (Column likely NULL). Skipping watermark update.")
            else:
                print(f"🕒 Updating watermark → {new_wm}")
                set_watermark(wm_key, new_wm, conn)