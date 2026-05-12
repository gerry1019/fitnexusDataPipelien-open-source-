from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/FitnessCenterSubscription"
SILVER_PATH = f"s3a://{bucket}/silver/fitness_center_subscription"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/fitness_center_subscription/_checkpoint"

spark = create_spark_session("Silver FitnessCenterSubscription")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

df_clean = (
    df_stream
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("price", col("price").cast("double"))
    .withColumn("startDate", to_timestamp("startDate"))
    .withColumn("endDate", to_timestamp("endDate"))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()