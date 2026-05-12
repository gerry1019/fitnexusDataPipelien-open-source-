# gold_booking_funnel_metrics.py

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG

# Initialize Configuration
bucket = S3_CONFIG["bucket"]
spark = create_spark_session("Gold Booking Funnel")

# 1. Extract Silver Data
session_df = (
    spark.read.format("delta")
    .load(f"s3a://{bucket}/silver/session")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

booking_df = (
    spark.read.format("delta")
    .load(f"s3a://{bucket}/silver/booking")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

# 2. Daily Aggregations
session_daily = (
    session_df
    .withColumn("date", to_date("createdAt"))
    .groupBy("date")
    .agg(count("*").alias("daily_sessions"))
)

booking_daily = (
    booking_df
    .withColumn("date", to_date("createdAt"))
    .groupBy("date")
    .agg(count("*").alias("daily_bookings"))
)

# 3. Join and Calculate Daily Conversion
# Left join ensures days with app activity but zero bookings aren't lost
gold_df = (
    session_daily
    .join(booking_daily, "date", "left")
    .fillna(0)
)

# 4. Add Time Dimensions
gold_df = gold_df.withColumn("year", year("date")) \
                 .withColumn("month", month("date")) \
                 .withColumn("week", weekofyear("date"))

# 5. Window Functions for Trends
# Weekly Window
win_weekly = Window.partitionBy("year", "week").orderBy("date")
# Monthly Window
win_monthly = Window.partitionBy("year", "month").orderBy("date")

# 6. Calculate MTD (Month-to-Date) and WTD (Week-to-Date) Metrics
gold_df = gold_df \
    .withColumn("sessions_wtd", sum("daily_sessions").over(win_weekly)) \
    .withColumn("bookings_wtd", sum("daily_bookings").over(win_weekly)) \
    .withColumn("sessions_mtd", sum("daily_sessions").over(win_monthly)) \
    .withColumn("bookings_mtd", sum("daily_bookings").over(win_monthly))

# 7. Final Conversion Rate Calculations
gold_df = gold_df \
    .withColumn(
        "daily_conversion_pct",
        round(safe_divide(col("daily_bookings"), col("daily_sessions")) * 100, 2)
    ) \
    .withColumn(
        "monthly_conversion_pct",
        round(safe_divide(col("bookings_mtd"), col("sessions_mtd")) * 100, 2)
    )

# 8. Add Audit Metadata
gold_df = add_gold_audit(gold_df, "gold_booking_funnel_metrics")

# 9. Load to Gold Layer
# Partitioning by Year/Month for performance
write_gold_overwrite(
    gold_df,
    f"s3a://{bucket}/gold/booking_funnel_metrics",
    partition_cols=["year", "month"]
)

spark.stop()