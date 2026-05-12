# 🎯 Purpose
#
# Tracks subscription business health (SaaS metrics)
#
# 📊 Key Metrics
# MRR (Monthly Recurring Revenue)
# ARR (Annual Recurring Revenue)
# active_subscriptions
# active_centers
# avg_monthly_price
# new_subscriptions
# cancelled_subscriptions
# 💼 Used For
# SaaS growth analysis
# Investor reporting
# Subscription trends



from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG


# -----------------------
# CONFIG
# -----------------------

bucket = S3_CONFIG["bucket"]

SILVER_SUB = f"s3a://{bucket}/silver/fitness_center_subscription"
SILVER_PLAN = f"s3a://{bucket}/silver/fitness_center_membership_plan"

GOLD_PATH = f"s3a://{bucket}/gold/platform_subscription_metrics"


spark = create_spark_session("Gold Platform Subscription Metrics")


# -----------------------
# LOAD SILVER
# -----------------------

sub_df = (
    spark.read.format("delta").load(SILVER_SUB)
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

plan_df = spark.read.format("delta").load(SILVER_PLAN)


# -----------------------
# JOIN PLAN
# -----------------------

sub_joined = (
    sub_df.join(
        plan_df,
        sub_df.membershipPlanId == plan_df.id,
        "left"
    )
)


# -----------------------
# MONTHLY PRICE
# -----------------------

sub_joined = (
    sub_joined.withColumn(
        "monthly_price",
        col("price") / (col("durationDays") / 30)
    )
)


# -----------------------
# ACTIVE SUBS
# -----------------------

active_df = sub_joined.filter(col("status") == "ACTIVE")


# -----------------------
# METRICS
# -----------------------

metrics_df = (
    active_df.agg(
        count("*").alias("active_subscriptions"),
        countDistinct("fitnessCenterId").alias("active_centers"),
        sum("monthly_price").alias("MRR"),
        avg("monthly_price").alias("avg_monthly_price")
    )
)


metrics_df = metrics_df.withColumn(
    "ARR",
    col("MRR") * 12
)


# -----------------------
# NEW / CANCELLED
# -----------------------

today = current_date()

new_subs = (
    sub_joined
    .filter(to_date("createdAt") == today)
    .count()
)

cancelled_subs = (
    sub_joined
    .filter(col("status") != "ACTIVE")
    .count()
)


metrics_df = (
    metrics_df
    .withColumn("new_subscriptions", lit(new_subs))
    .withColumn("cancelled_subscriptions", lit(cancelled_subs))
)


# -----------------------
# DQ
# -----------------------

assert_non_negative(
    metrics_df,
    "MRR",
    "gold_platform_subscription_metrics"
)


# -----------------------
# AUDIT
# -----------------------

metrics_df = add_gold_audit(
    metrics_df,
    "gold_platform_subscription_metrics"
)


# -----------------------
# WRITE
# -----------------------

write_gold_overwrite(
    df=metrics_df,
    output_path=GOLD_PATH
)


print("✅ gold_platform_subscription_metrics updated")

spark.stop()