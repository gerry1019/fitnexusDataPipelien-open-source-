from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG

# -------------------------------
# CONFIG
# -------------------------------
bucket = S3_CONFIG["bucket"]
SILVER_SESSION = f"s3a://{bucket}/silver/session"
SILVER_BOOKING = f"s3a://{bucket}/silver/booking"
GOLD_PATH = f"s3a://{bucket}/gold/session_utilization"

spark = create_spark_session("Gold Session Utilization")

# -------------------------------
# LOAD SILVER
# -------------------------------
# FIX: Filter out records that are missing the FK at the source
# to avoid DQ failures later.
session_df = (
    spark.read.format("delta").load(SILVER_SESSION)
    .filter(col("fitnessCenterId").isNotNull())
)

booking_df = (
    spark.read.format("delta").load(SILVER_BOOKING)
)

# -------------------------------
# AGGREGATE BOOKINGS
# -------------------------------
booking_count = (
    booking_df
    .groupBy("sessionId")
    .agg(count("*").alias("booked_count"))
)

# -------------------------------
# GOLD TRANSFORMATION
# -------------------------------
gold_df = (
    session_df
    .join(
        booking_count,
        session_df.id == booking_count.sessionId,
        "inner" # CHANGED: Using inner join to only show sessions with bookings
    )
    .withColumn(
        "utilization_percentage",
        round(
            safe_divide(
                col("booked_count"),
                col("availableSlots")
            ) * 100,
            2
        )
    )
    .withColumn(
        "remaining_slots",
        col("availableSlots") - col("booked_count")
    )
    .withColumn(
        "occupancy_status",
        when(col("utilization_percentage") >= 100, "Full")
        .when(col("utilization_percentage") >= 80, "High")
        .otherwise("Available")
    )
    .select(
        session_df.id.alias("sessionId"),
        "fitnessCenterId",
        "availableSlots",
        "booked_count",
        "remaining_slots",
        "utilization_percentage",
        "occupancy_status"
    )
)

# -------------------------------
# REMOVE DUPLICATES
# -------------------------------
gold_df = gold_df.dropDuplicates(["sessionId"])

# -------------------------------
# DATA QUALITY CHECKS
# -------------------------------
# This will now pass because of the .filter() and "inner" join logic
assert_not_null(
    gold_df,
    ["sessionId", "fitnessCenterId"],
    "gold_session_utilization"
)

assert_non_negative(
    gold_df,
    "utilization_percentage",
    "gold_session_utilization"
)

# Business rule check
if gold_df.filter(col("booked_count") > col("availableSlots")).limit(1).count() > 0:
    raise Exception("[DQ FAIL] booked_count > availableSlots in gold_session_utilization")

# -------------------------------
# ADD AUDIT & UPSERT
# -------------------------------
gold_df = add_gold_audit(gold_df, "gold_session_utilization")

write_gold_upsert(
    spark=spark,
    df=gold_df,
    output_path=GOLD_PATH,
    primary_key="sessionId"
)

print(f"✅ Gold table updated at {GOLD_PATH}")
spark.stop()