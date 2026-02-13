# ------------------------------------------------------
# Load shared Silver utilities
# ------------------------------------------------------
%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/silver-transformations/silver_utils.py


# ------------------------------------------------------
# Unity Catalog context
# ------------------------------------------------------
spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA silver")


from pyspark.sql import functions as F
from pyspark.sql.functions import *


# ------------------------------------------------------
# Table configuration
# ------------------------------------------------------
PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

BRONZE_PATH = "abfss://bronze@fitnexusdatalake.dfs.core.windows.net/FitnessCenter"
SCHEMA_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/FitnessCenter/schema"
CHECKPOINT_PATH = "abfss://silver@fitnexusdatalake.dfs.core.windows.net/FitnessCenter/checkpoint"

SILVER_TABLE = "fitness_center"


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
# Silver Transformations
# ------------------------------------------------------

email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

df_clean = (
    df_stream

    # --- A. Normalize text fields ---
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("category", upper(trim(col("category"))))

    # Handle empty description
    .withColumn(
        "description",
        when(length(trim(col("description"))) == 0, lit(None))
        .otherwise(col("description"))
    )

    # --- B. Email validation ---
    .withColumn("officalemail", lower(trim(col("officalemail"))))
    .withColumn(
        "officalemail",
        when(col("officalemail").rlike(email_pattern), col("officalemail"))
        .otherwise(lit(None))
    )

    # --- C. Phone validation ---
    .withColumn("phone_clean", regexp_replace(trim(col("phone")), r"[^0-9]", ""))
    .withColumn(
        "phone",
        when(length(col("phone_clean")).between(9, 11), col("phone_clean"))
        .otherwise(lit(None))
    )
    .drop("phone_clean")

    # --- D. Type casting & timestamps ---
    .withColumn("isVerified", col("isVerified").cast("boolean"))
    .withColumn("isBlocked", col("isBlocked").cast("boolean"))
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .withColumn("lastLoginAt", to_timestamp("lastLoginAt"))

    # --- E. Data quality ---
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())

    # --- Drop rescued data ---
    .drop("_rescued_data")
)


# ------------------------------------------------------
# Final schema
# ------------------------------------------------------
final_df = df_clean.select(
    "id",
    "name",
    "category",
    "officalemail",
    "phone",
    "isVerified",
    "description",
    "addressId",
    "createdAt",
    "updatedAt",
    "isBlocked",
    "lastLoginAt"
)


# ------------------------------------------------------
# Write to Silver (MERGE / UPSERT)
# ------------------------------------------------------
run_silver_stream(
    final_df,
    spark,
    CHECKPOINT_PATH,
    SILVER_TABLE,
    PRIMARY_KEY,
    UPDATED_COL
)
