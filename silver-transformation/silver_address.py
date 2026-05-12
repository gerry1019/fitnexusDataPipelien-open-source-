from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/Address"
SILVER_PATH = f"s3a://{bucket}/silver/address"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/address/_checkpoint"

spark = create_spark_session("Silver Address")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

df_clean = (
    df_stream
    .withColumn("street", initcap(trim(col("street"))))
    .withColumn("city", initcap(trim(col("city"))))
    .withColumn("state", initcap(trim(col("state"))))
    .withColumn("country", initcap(trim(col("country"))))
    .withColumn("postalCode", regexp_replace(col("postalCode"), r"[^0-9]", ""))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()