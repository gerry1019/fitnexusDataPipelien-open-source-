%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")

from pyspark.sql.functions import *

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/MembershipPlan"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/MembershipPlan/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/MembershipPlan/checkpoint"

SILVER_TABLE = "membership_plan"

df_stream = read_bronze_stream(spark, BRONZE_PATH, SCHEMA_PATH)

df_clean = (
    df_stream
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("description",
        when(length(trim(col("description"))) == 0, None)
        .otherwise(trim(col("description")))
    )
    .withColumn("features",
        split(
            regexp_replace(regexp_replace("features", r"[\{\}]", ""), r"\s*,\s*", ","),
            ","
        )
    )
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("price", col("price").cast("double"))
    .withColumn("durationDays", col("durationDays").cast("int"))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
    .drop("_rescued_data")
)

final_df = df_clean.select(
    "id","fitnessCenterId","name","description",
    "price","durationDays","features",
    "status","createdAt","updatedAt"
)

run_silver_stream(final_df, spark, CHECKPOINT_PATH, SILVER_TABLE, PRIMARY_KEY, UPDATED_COL)
