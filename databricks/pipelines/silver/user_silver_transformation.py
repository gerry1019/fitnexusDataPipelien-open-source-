%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py


spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/User"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/User/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/User/checkpoint"

SILVER_TABLE = "user"


df_stream = read_bronze_stream(
    spark,
    BRONZE_PATH,
    SCHEMA_PATH
)

print(df_stream.isStreaming)  # must print True


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
    .filter(col("updatedAt").isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop(
        "password",
        "refreshToken",
        "loginAttempts",
        "lastLoginAt"
    )
)

run_silver_stream(
    df_clean,
    spark,
    CHECKPOINT_PATH,
    SILVER_TABLE,
    PRIMARY_KEY,
    UPDATED_COL
)
