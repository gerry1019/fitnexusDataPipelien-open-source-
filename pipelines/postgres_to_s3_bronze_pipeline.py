from pyspark.sql.functions import col, max as spark_max
from datetime import datetime

from config.pipeline_config import TABLE_LIST
from config.db_config import get_jdbc_url, DB_CONFIG
from config.s3_config import S3_CONFIG
from utils.s3_utils import get_watermark, set_watermark, ensure_bucket_exists
from utils.spark_utils import create_spark_session


def run_full_ingestion():

    spark = create_spark_session("Fitnexus Bronze Ingestion")

    ensure_bucket_exists()

    for table_name, meta in TABLE_LIST.items():

        print("\n" + "=" * 60)
        print(f"🚀 Processing table: {table_name}")
        print("=" * 60)

        updated_column = meta["updated_column"]
        full_load_flag = meta.get("full_load", False)

        last_wm = get_watermark(table_name)

        quoted_table = f'"{table_name}"'
        quoted_updated_column = f'"{updated_column}"'

        # ---------------------------------------------------------
        # STEP 1: PREPARE QUERY
        # ---------------------------------------------------------

        if full_load_flag or last_wm is None:
            print(f"⚠️ FULL LOAD for {table_name}")
            query = f"(SELECT * FROM {quoted_table}) as t"
        else:
            print(f"🚀 INCREMENTAL LOAD from watermark → {last_wm}")
            query = (
                f"(SELECT * FROM {quoted_table} "
                f"WHERE {quoted_updated_column} > '{last_wm}') as t"
            )

        # ---------------------------------------------------------
        # STEP 2: READ USING SPARK JDBC
        # ---------------------------------------------------------

        df = (
            spark.read.format("jdbc")
            .option("url", get_jdbc_url())
            .option("dbtable", query)
            .option("user", DB_CONFIG["user"])
            .option("password", DB_CONFIG["password"])
            .option("driver", "org.postgresql.Driver")
            .option("fetchsize", "10000")
            .load()
        )

        # Proper empty check
        if df.head(1) == []:
            print(f"🟡 No new data found for table → {table_name}")
            continue

        record_count = df.count()
        print(f"📊 Records fetched → {record_count}")

        # ---------------------------------------------------------
        # STEP 3: WRITE TO BRONZE (DATE + TABLE STRUCTURE)
        # ---------------------------------------------------------

        current_date_str = datetime.now().strftime("%Y-%m-%d")

        bronze_path = (
            f"s3a://{S3_CONFIG['bucket']}/bronze/"
            f"{table_name}/{current_date_str}_{table_name}"
        )

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )

        print(f"🪣 Data written to → {bronze_path}")

        # ---------------------------------------------------------
        # STEP 4: UPDATE WATERMARK
        # ---------------------------------------------------------

        new_wm = df.select(spark_max(col(updated_column))).collect()[0][0]

        if new_wm:
            set_watermark(table_name, str(new_wm))
            print(f"🕒 Watermark updated → {new_wm}")

    spark.stop()
    print("\n✅ Bronze ingestion completed successfully.\n")


if __name__ == "__main__":
    run_full_ingestion()
