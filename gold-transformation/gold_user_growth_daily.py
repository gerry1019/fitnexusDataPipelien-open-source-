# gold_user_growth_daily.py

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG

bucket = S3_CONFIG["bucket"]
spark = create_spark_session("Gold User Growth")

# 1. Extract Silver Data
user_df = (
    spark.read.format("delta")
    .load(f"s3a://{bucket}/silver/user")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

# 2. Daily Aggregation
gold_df = (
    user_df
    .withColumn("date", to_date("createdAt"))
    .groupBy("date")
    .agg(count("*").alias("new_users_daily"))
)

# 3. Add Time Dimensions (Post-Aggregation to keep columns)
gold_df = gold_df.withColumn("year", year("date")) \
                 .withColumn("month", month("date")) \
                 .withColumn("week", weekofyear("date"))

# 4. Define Windows
# Cumulative (All time)
win_total = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

# Resets every week
win_weekly = Window.partitionBy("year", "week").orderBy("date")

# Resets every month
win_monthly = Window.partitionBy("year", "month").orderBy("date")

# Standard Lag Window (For day-over-day growth)
win_daily_lag = Window.orderBy("date")

# 5. Calculate Metrics
gold_df = gold_df \
    .withColumn("total_users_lifetime", sum("new_users_daily").over(win_total)) \
    .withColumn("new_users_weekly_mtd", sum("new_users_daily").over(win_weekly)) \
    .withColumn("new_users_monthly_mtd", sum("new_users_daily").over(win_monthly))

# 6. Calculate Day-over-Day Growth Percentage
# Using safe_divide to ensure no crashes on days with 0 users
gold_df = gold_df.withColumn(
    "user_growth_daily_pct",
    round(
        safe_divide(
            (col("new_users_daily") - lag("new_users_daily").over(win_daily_lag)),
            lag("new_users_daily").over(win_daily_lag)
        ) * 100,
        2
    )
)

# 7. Add Audit Metadata
gold_df = add_gold_audit(gold_df, "gold_user_growth_daily")

# 8. Load to Gold Layer
# Partitioning by year and month for better query performance and file sizes
write_gold_overwrite(
    gold_df,
    f"s3a://{bucket}/gold/user_growth_daily",
    partition_cols=["year", "month"]
)

spark.stop()

