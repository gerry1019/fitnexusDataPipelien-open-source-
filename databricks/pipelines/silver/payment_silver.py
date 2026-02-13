%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/Payment"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Payment/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/Payment/checkpoint"

SILVER_TABLE = "payment"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("provider", upper(trim(col("provider"))))
    .withColumn("amount", col("amount").cast("double"))
    .withColumn("paymentId", regexp_replace(col("paymentId"), r'^"+|"+$', ""))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("_rescued_data")
)

final_df = df_clean.select(
    "id","userId","membershipPlanId","amount",
    "currency","status","provider","paymentId",
    "createdAt","updatedAt"
)

run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
