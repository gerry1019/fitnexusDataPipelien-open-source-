%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/Session"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Session/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Session/checkpoint"

SILVER_TABLE = "session"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("sessionType", upper(trim(col("sessionType"))))
    .withColumn("startTime", to_timestamp("startTime"))
    .withColumn("endTime", to_timestamp("endTime"))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .withColumn(
        "durationMinutes",
        round((col("endTime").cast("long") - col("startTime").cast("long")) / 60)
    )
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("_rescued_data")
)

final_df = df_clean.select(
    "id","name","sessionType",
    "startTime","endTime","durationMinutes",
    "maxCapacity","trainerId","fitnessCenterId",
    "createdAt","updatedAt"
)

run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
