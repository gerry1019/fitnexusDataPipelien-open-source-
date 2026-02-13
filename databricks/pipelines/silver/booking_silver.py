%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/Booking"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Booking/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Booking/checkpoint"

SILVER_TABLE = "booking"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("bookingDate", to_timestamp("bookingDate"))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col("userId").isNotNull())
    .filter(col("sessionId").isNotNull())
    .filter(col("fitnessCenterId").isNotNull())
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("_rescued_data")
)

final_df = df_clean.select(
    "id","userId","sessionId","fitnessCenterId",
    "bookingDate","status","createdAt","updatedAt"
)

run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
