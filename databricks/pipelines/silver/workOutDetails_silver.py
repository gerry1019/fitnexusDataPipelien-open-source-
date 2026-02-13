%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/WorkOutDetails"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/WorkOutDetails/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/WorkOutDetails/checkpoint"

SILVER_TABLE = "workout_details"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("goal", upper(trim(col("goal"))))
    .withColumn("exercises", split(regexp_replace(trim(col("exercises")), r"\s*,\s*", ","), ","))
    .withColumn("equipment", split(regexp_replace(trim(col("equipment")), r"\s*,\s*", ","), ","))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("_rescued_data")
)

final_df = df_clean.select(
    "id",
    "title",
    "mode",
    "startTime",
    "endTime",
    "durationMinutes",
    "availableSlots",
    "fitnessCenterId",
    "createdAt",
    "updatedAt"
)


run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
