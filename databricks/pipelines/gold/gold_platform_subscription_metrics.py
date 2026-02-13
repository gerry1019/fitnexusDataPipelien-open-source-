from pyspark.sql.functions import *

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA gold")

payment_df = spark.table("silver.payment")
subscription_df = spark.table("silver.fitnesscentersubscription")
plan_df = spark.table("silver.fitnesscentermembershipplan")

platform_payments = (
    payment_df
    .filter(col("userId").isNull() & (col("status") == "SUCCESS"))
    .withColumn("date", to_date("createdAt"))
)

daily_revenue = (
    platform_payments
    .groupBy("date")
    .agg(sum("amount").alias("daily_platform_revenue"))
)

active_subs = (
    subscription_df
    .filter(col("status") == "ACTIVE")
    .join(plan_df, subscription_df.planId == plan_df.id, "left")
    .withColumn(
        "monthly_price",
        when(col("durationDays") >= 365, plan_df["price"] / 12)
        .when(col("durationDays") >= 90, plan_df["price"] / 3)
        .otherwise(plan_df["price"])
    )
)

mrr_df = active_subs.agg(
    sum("monthly_price").alias("MRR"),
    countDistinct("fitnessCenterId").alias("active_centers")
)

gold_platform_subscription_metrics = (
    daily_revenue
    .crossJoin(mrr_df)
    .withColumn("ARR", col("MRR") * 12)
    .withColumn(
        "avg_revenue_per_center",
        round(col("MRR") / col("active_centers"), 2)
    )
)

gold_platform_subscription_metrics = add_snapshot_columns(
    gold_platform_subscription_metrics
)

write_gold_table(
    spark,
    gold_platform_subscription_metrics,
    "gold_platform_subscription_metrics",
    partition_cols=["date"]
)