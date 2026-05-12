from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/FitnessCenter"
SILVER_PATH = f"s3a://{bucket}/silver/fitness_center"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/fitness_center/_checkpoint"

spark = create_spark_session("Silver FitnessCenter")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

df_clean = (
    df_stream
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("officalemail", lower(trim(col("officalemail"))))
    .withColumn("officalemail",
        when(col("officalemail").rlike(email_pattern), col("officalemail")).otherwise(None))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()