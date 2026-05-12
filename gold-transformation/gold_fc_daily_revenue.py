# 🎯 Purpose
#
# Tracks how much each fitness center earns daily
#
# 📊 Key Metrics
# date
# fitnessCenterId
# daily_revenue
# 💼 Used For
# Center performance tracking
# Revenue trends
# Compare centers

from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG

# -----------------------
# CONFIG
# -----------------------

bucket = S3_CONFIG["bucket"]

SILVER_PAYMENT = f"s3a://{bucket}/silver/payment"

GOLD_PATH = f"s3a://{bucket}/gold/fc_daily_revenue"


spark = create_spark_session("Gold FC Daily Revenue")


# -----------------------
# LOAD SILVER
# -----------------------

payment_df = (
    spark.read.format("delta").load(SILVER_PAYMENT)
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)


# -----------------------
# GOLD LOGIC
# -----------------------

gold_df = (
    payment_df
    .filter(col("status") == "SUCCESS")
    .filter(col("userId").isNotNull())
    .withColumn(
        "date",
        to_date(col("createdAt"))
    )
    .groupBy(
        "fitnessCenterId",
        "date"
    )
    .agg(
        sum("amount").alias("daily_revenue")
    )
)

# -----------------------
# DATA QUALITY
# -----------------------

assert_not_null(
    gold_df,
    ["fitnessCenterId", "date"],
    "gold_fc_daily_revenue"
)

assert_non_negative(
    gold_df,
    "daily_revenue",
    "gold_fc_daily_revenue"
)


# -----------------------
# AUDIT
# -----------------------

gold_df = add_gold_audit(
    gold_df,
    "gold_fc_daily_revenue"
)


# -----------------------
# WRITE (overwrite + partition)
# -----------------------

write_gold_overwrite(
    df=gold_df,
    output_path=GOLD_PATH,
    partition_cols=["date"]
)


print("✅ gold_fc_daily_revenue updated")

spark.stop()