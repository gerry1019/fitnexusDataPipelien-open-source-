from pyspark.sql.functions import *

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA gold")

booking_df = spark.table("silver.booking")

gold_booking_funnel = (
    booking_df
    .withColumn("date", to_date("createdAt"))
    .groupBy("date")
    .agg(
        count("*").alias("total_bookings"),
        sum(when(col("status") == "COMPLETED", 1).otherwise(0)).alias("completed"),
        sum(when(col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled")
    )
)

gold_booking_funnel = add_snapshot_columns(gold_booking_funnel)

write_gold_table(
    spark,
    gold_booking_funnel,
    "gold_booking_funnel",
    partition_cols=["date"]
)
