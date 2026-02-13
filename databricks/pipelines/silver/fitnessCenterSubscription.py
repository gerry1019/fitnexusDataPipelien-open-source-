%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

# -------------------------
# CONFIG
# -------------------------
PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/FitnessCenterSubscription"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/FitnessCenterSubscription/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/FitnessCenterSubscription/checkpoint"

SILVER_TABLE = "fitnesscentersubscription"

# -------------------------
# READ BRONZE (STREAM)
# -------------------------
df_stream = read_bronze_stream(
    spark,
    BRONZE_PATH,
    SCHEMA_PATH
)

# -------------------------
# TRANSFORM
# -------------------------
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

final_df = df_clean.select(
    "id",
    "fitnessCenterId",
    "membershipPlanId",
    "startDate",
    "endDate",
    "price",
    "status",
    "createdAt",
    "updatedAt"
)

# -------------------------
# WRITE TO SILVER
# -------------------------
run_silver_stream(
    final_df,
    spark,
    CHECKPOINT_PATH,
    SILVER_TABLE,
    PRIMARY_KEY,
    UPDATED_COL
)
