%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/Address"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Address/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Address/checkpoint"

SILVER_TABLE = "address"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("street", initcap(trim(col("street"))))
    .withColumn("city", initcap(trim(col("city"))))
    .withColumn("state", initcap(trim(col("state"))))
    .withColumn("country", initcap(trim(col("country"))))
    .withColumn("landmark",
        when(length(trim(col("landmark"))) == 0, None)
        .otherwise(initcap(trim(col("landmark"))))
    )
    .withColumn("pincode_clean", regexp_replace(col("pincode"), r"[^0-9]", ""))
    .withColumn("pincode",
        when(length(col("pincode_clean")) > 0, col("pincode_clean")).otherwise(None)
    )
    .drop("pincode_clean")
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("_rescued_data")
)

final_df = df_clean.select(
    "id","street","city","state","pincode",
    "country","landmark","createdAt","updatedAt"
)

run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
