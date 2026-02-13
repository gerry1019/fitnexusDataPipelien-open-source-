%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/Staff"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Staff/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Staff/checkpoint"

SILVER_TABLE = "staff"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("firstName", initcap(trim(col("firstName"))))
    .withColumn("lastName", initcap(trim(col("lastName"))))
    .withColumn("fullName", concat_ws(" ", col("firstName"), col("lastName")))
    .withColumn("role", upper(trim(col("role"))))
    .withColumn("bio", when(length(trim(col("bio"))) == 0, None).otherwise(col("bio")))
    .withColumn("age", col("age").cast("int"))
    .withColumn("experience", col("experience").cast("int"))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("certifications","_rescued_data")
)

final_df = df_clean.select(
    "id","firstName","lastName","fullName",
    "age","role","experience","bio",
    "addressId","userId","fitnessCenterId",
    "createdAt","updatedAt"
)

run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
