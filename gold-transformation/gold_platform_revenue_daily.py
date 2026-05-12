# 🎯 Purpose
#
# Tracks platform earnings (Fitnexus income)
#
# 📊 Key Metrics
# date
# platform_revenue
# transaction_count
# paid_centers
# 💼 Used For
# Business growth tracking
# Daily revenue monitoring
# Investor metrics

from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG


# -----------------------
# CONFIG
# -----------------------

bucket = S3_CONFIG["bucket"]

SILVER_PAYMENT = f"s3a://{bucket}/silver/payment"

GOLD_PATH = f"s3a://{bucket}/gold/platform_revenue_daily"


spark = create_spark_session("Gold Platform Revenue Daily")


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
    .filter(col("userId").isNull())
    .withColumn(
        "date",
        to_date(col("createdAt"))
    )
    .groupBy("date")
    .agg(
        sum("amount").alias("platform_revenue"),
        count("*").alias("transaction_count"),
        countDistinct("fitnessCenterId").alias("paid_centers")
    )
)


# -----------------------
# DATA QUALITY
# -----------------------

assert_not_null(
    gold_df,
    ["date"],
    "gold_platform_revenue_daily"
)

assert_non_negative(
    gold_df,
    "platform_revenue",
    "gold_platform_revenue_daily"
)


# -----------------------
# AUDIT
# -----------------------

gold_df = add_gold_audit(
    gold_df,
    "gold_platform_revenue_daily"
)


# -----------------------
# WRITE GOLD
# -----------------------

write_gold_overwrite(
    df=gold_df,
    output_path=GOLD_PATH,
    partition_cols=["date"]
)


print("✅ gold_platform_revenue_daily updated")

spark.stop()