# ------------------------------------------------------
# Load shared silver utilities
# ------------------------------------------------------
%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py


# ------------------------------------------------------
# Set Unity Catalog context
# ------------------------------------------------------
spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")


from pyspark.sql import functions as F
from pyspark.sql.functions import *


# ------------------------------------------------------
# Table configuration (ONLY THIS CHANGES PER TABLE)
# ------------------------------------------------------
PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/UserInfo"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/UserInfo/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/UserInfo/checkpoint"

SILVER_TABLE = "user_info"


# ------------------------------------------------------
# Read Bronze as STREAM
# ------------------------------------------------------
df_stream = read_bronze_stream(
    spark,
    BRONZE_PATH,
    SCHEMA_PATH
)

print(df_stream.isStreaming)  # must be True


# ------------------------------------------------------
# Silver Transformations (YOUR LOGIC)
# ------------------------------------------------------
df_clean = (
    df_stream

    # -------------------------
    # Normalize Names
    # -------------------------
    .withColumn("firstName", initcap(trim(col("firstName"))))
    .withColumn("lastName", initcap(trim(col("lastName"))))

    # -------------------------
    # Fix invalid height/weight
    # -------------------------
    .withColumn(
        "height",
        when(col("height") <= 0, lit(None)).otherwise(col("height"))
    )
    .withColumn(
        "weight",
        when(col("weight") <= 0, lit(None)).otherwise(col("weight"))
    )

    # -------------------------
    # Full Name
    # -------------------------
    .withColumn(
        "fullName",
        concat_ws(" ", col("firstName"), col("lastName"))
    )

    # -------------------------
    # BMI calculation
    # BMI = weight / (height in meters)^2
    # -------------------------
    .withColumn(
        "BMI",
        round(col("weight") / ((col("height") / 100) ** 2), 2)
    )
    .withColumn(
        "BMI",
        when(
            col("height").isNull() | col("weight").isNull(),
            lit(None)
        ).otherwise(col("BMI"))
    )

    # -------------------------
    # Standardize timestamps
    # -------------------------
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))

    # -------------------------
    # Metadata
    # -------------------------
    .withColumn("record_source", lit("postgres_bronze"))
    .withColumn("load_timestamp", current_timestamp())

    # -------------------------
    # Data quality filters
    # -------------------------
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())

    # -------------------------
    # Drop Auto Loader rescue column
    # -------------------------
    .drop("_rescued_data")
)


# ------------------------------------------------------
# Write to Silver (MERGE / UPSERT)
# ------------------------------------------------------
run_silver_stream(
    df_clean,
    spark,
    CHECKPOINT_PATH,
    SILVER_TABLE,
    PRIMARY_KEY,
    UPDATED_COL
)
