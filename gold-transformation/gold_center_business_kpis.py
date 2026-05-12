# Gives complete performance of each fitness center
#
# 📊 Key Metrics
# total_users
# total_bookings
# total_sessions
# total_revenue
# active_subscription
# days_since_login
# lifecycle_status
# 💼 Used For
# Full dashboard view
# Identify top / weak centers
# Business decisions
# Retention tracking



from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG

# -----------------------
# CONFIG
# -----------------------

bucket = S3_CONFIG["bucket"]
spark=create_spark_session("Gold Center Business KPIS")

# -----------------------
# LOAD SILVER TABLES
# -----------------------

fc=(
    spark.read.format("delta").load(f"s3a://{bucket}/silver/fitness_center")
    # .filter(coalesce(col("isDeleted"),lit(False))==False)
)

user = (
    spark.read.format("delta").load(f"s3a://{bucket}/silver/user")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

booking = (
    spark.read.format("delta").load(f"s3a://{bucket}/silver/booking")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

session = (
    spark.read.format("delta").load(f"s3a://{bucket}/silver/session")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

payment = (
    spark.read.format("delta").load(f"s3a://{bucket}/silver/payment")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

sub = (
    spark.read.format("delta").load(f"s3a://{bucket}/silver/fitness_center_subscription")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

# -----------------------
# BASE TABLE (fix id)
# -----------------------

fc_base = (
    fc
    .withColumnRenamed("id","fitnessCenterId")
)

# -----------------------
# USERS
# -----------------------

user_count = (
    user
    .groupBy("fitnessCenterId")
    .agg(count("*").alias("total_users"))
)

# -----------------------
# BOOKINGS
# -----------------------

booking_count = (
    booking
    .groupBy("fitnessCenterId")
    .agg(count("*").alias("total_booking"))
)

# -----------------------
# SESSIONS
# -----------------------

session_count = (
    session
    .groupBy("fitnessCenterId")
    .agg(count("*").alias("total_sessions"))
)

# -----------------------
# REVENUE (USER PAYMENTS ONLY)
# -----------------------

revenue = (
    payment
    .filter((col("status")== "SUCCESS") & col("userId").isNotNull())
    .groupBy("fitnessCenterId")
    .agg(sum("amount").alias("total_revenue"))
)

# -----------------------
# ACTIVE SUBSCRIPTIONS
# -----------------------

active_sub=(
    sub
    .filter(col("status")=="ACTIVE")
    .groupBy("fitnessCenterId")
    .agg(count("*").alias("active_subscription"))
)

# -----------------------
# LAST LOGIN
# -----------------------

fc_base = (
    fc_base
    .withColumn(
        "days_since_login",
        when(
            col("lastLoginAt").isNull(),
            lit(999)
        ).otherwise(
            datediff(current_date(),to_date("lastLoginAt"))
        )
    )
)

# -----------------------
# JOIN ALL KPIs
# -----------------------

gold_df = (
    fc_base
    .join(user_count, "fitnessCenterId", "left")
    .join(booking_count, "fitnessCenterId", "left")
    .join(session_count, "fitnessCenterId", "left")
    .join(revenue, "fitnessCenterId", "left")
    .join(active_sub, "fitnessCenterId", "left")
)

# -----------------------
# FILL NULLS
# -----------------------

gold_df = gold_df.fillna({
    "total_users": 0,
    "total_bookings": 0,
    "total_sessions": 0,
    "total_revenue": 0,
    "active_subscription": 0
})

# -----------------------
# LIFECYCLE STATUS
# -----------------------

gold_df=(
    gold_df
    .withColumn(
        "lifecycle_status",
        when(col('active_subscription') == 0, "NO_SUBSCRIPTION")
        .when(col("days_since_login") > 30 , "INACTIVE")
        .when(col("total_users")==0,"NO_USERS")
        .otherwise("ACTIVE")
    )
)

# -----------------------
# SELECT FINAL SCHEMA
# -----------------------

gold_df = gold_df.select(
    "fitnessCenterId",
    "name",
    "total_users",
    "total_bookings",
    "total_sessions",
    "total_revenue",
    "active_subscription",
    "days_since_login",
    "lifecycle_status"
)

# -----------------------
# DATA QUALITY
# -----------------------

assert_not_null(
    gold_df,
    ["fitnessCenterId"],
    "gold_center_business_kpis"
)

assert_non_negative(
    gold_df,
    "total_revenue",
    "gold_center_business_kpis"
)


# -----------------------
# AUDIT
# -----------------------

gold_df = add_gold_audit(
    gold_df,
    "gold_center_business_kpis"
)


# -----------------------
# WRITE GOLD
# -----------------------

write_gold_overwrite(
    df=gold_df,
    output_path=f"s3a://{bucket}/gold/center_business_kpis"
)


print("✅ gold_center_business_kpis updated")

spark.stop()

