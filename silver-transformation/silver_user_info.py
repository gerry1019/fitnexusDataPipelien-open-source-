from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/UserInfo"
SILVER_PATH = f"s3a://{bucket}/silver/user_info"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/user_info/_checkpoint"

spark = create_spark_session("Silver UserInfo")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

df_clean = (
    df_stream
    .withColumn("firstName", initcap(trim(col("firstName"))))
    .withColumn("lastName", initcap(trim(col("lastName"))))
    .withColumn("height", when(col("height") <= 0, None).otherwise(col("height")))
    .withColumn("weight", when(col("weight") <= 0, None).otherwise(col("weight")))
    .withColumn("fullName", concat_ws(" ", col("firstName"), col("lastName")))
    .withColumn("BMI", round(col("weight") / ((col("height") / 100) ** 2), 2))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()