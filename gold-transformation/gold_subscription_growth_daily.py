# gold_subscription_growth_daily.py

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from utils.spark_utils import create_spark_session
from utils.gold_utils import *
from config.s3_config import S3_CONFIG

bucket = S3_CONFIG["bucket"]
spark = create_spark_session("Gold Subscription Growth")

# 1. Extract Silver Data
sub_df = (
    spark.read.format("delta")
    .load(f"s3a://{bucket}/silver/fitness_center_subscription")
    # .filter(coalesce(col("isDeleted"), lit(False)) == False)
)

# 2. Daily Aggregations (The Base)
new_df = (
    sub_df.withColumn("date", to_date("createdAt"))
    .groupBy("date").agg(count("*").alias("new_sub_daily"))
)

churn_df = (
    sub_df.filter(col("status") != "ACTIVE")
    .withColumn("date", to_date("updatedAt"))
    .groupBy("date").agg(count("*").alias("churn_sub_daily"))
)

# 3. Join & Clean
gold_df = new_df.join(churn_df, "date", "full").fillna(0)

# 4. Add Time Dimensions for Grouping
gold_df = gold_df.withColumn("year", year("date")) \
                 .withColumn("month", month("date")) \
                 .withColumn("week", weekofyear("date"))

# 5. Window Specifications
# Cumulative (All time)
win_total = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

# Weekly Window (Resets every week)
win_weekly = Window.partitionBy("year", "week").orderBy("date")

# Monthly Window (Resets every month)
win_monthly = Window.partitionBy("year", "month").orderBy("date")

# 6. Calculate Metrics
gold_df = gold_df \
    .withColumn("total_active_base", sum(col("new_sub_daily") - col("churn_sub_daily")).over(win_total)) \
    .withColumn("new_sub_weekly_mtd", sum("new_sub_daily").over(win_weekly)) \
    .withColumn("new_sub_monthly_mtd", sum("new_sub_daily").over(win_monthly)) \
    .withColumn("churn_sub_weekly_mtd", sum("churn_sub_daily").over(win_weekly))

# 7. Calculate Churn Rate (Daily)
gold_df = gold_df.withColumn(
    "daily_churn_rate_pct",
    round(safe_divide(col("churn_sub_daily"), col("total_active_base")) * 100, 2)
)

# 8. Final Polish & Audit
gold_df = add_gold_audit(gold_df, "gold_subscription_growth_daily")

# 9. Load to Gold Layer
write_gold_overwrite(
    gold_df,
    f"s3a://{bucket}/gold/subscription_growth_daily",
    partition_cols=["year", "month"] # Better partitioning for growth tables
)

spark.stop()