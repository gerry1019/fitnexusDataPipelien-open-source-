from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/Payment"
SILVER_PATH = f"s3a://{bucket}/silver/payment"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/payment/_checkpoint"

spark = create_spark_session("Silver Payment")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

df_clean = (
    df_stream
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("method", upper(trim(col("method"))))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("transactionId", regexp_replace(col("transactionId"), r'^"+|"+$', ""))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()