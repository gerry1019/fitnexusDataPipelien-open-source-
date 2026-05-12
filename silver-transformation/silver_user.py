from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/User"
SILVER_PATH = f"s3a://{bucket}/silver/user"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/user/_checkpoint"

spark = create_spark_session("Silver User")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

df_clean = (
    df_stream
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .withColumn("lastLoginAt", to_timestamp("lastLoginAt"))
    .withColumn("isVerified", coalesce(col("isVerified"), lit(False)).cast("boolean"))
    .withColumn("isBlocked", coalesce(col("isBlocked"), lit(False)).cast("boolean"))
    .withColumn("phone", regexp_replace(col("phone").cast("string"), r"[^0-9]", ""))
    .withColumn("email", lower(col("email")))
    .withColumn("email", when(col("email").isNull(), "unknown").otherwise(col("email")))
    .withColumn("role", upper(col("role")))
    .withColumn("role", when(col("role").isNull(), "USER").otherwise(col("role")))
    .withColumn("fitnessCenterId", col("fitnessCenterId").cast("int"))
    .withColumn("createdDate", to_date(col("createdAt")))
    .withColumn("updatedDate", to_date(col("updatedAt")))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("password","refreshToken","loginAttempts","_rescued_data")
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()