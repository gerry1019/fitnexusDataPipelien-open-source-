from pyspark.sql.functions import *

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA gold")

payment_df = spark.table("silver.payment")

gold_fitnesscenter_revenue = (
    payment_df
    .filter((col("status")=="SUCCESS") & (col("userId").isNotNull()))
    .withColumn("date",to_date("createdAt"))
    .groupBy("fitnessCenterId","date")
    .agg( sum("amount").alias("daily_revenue"),countDistinct("userId").alias("paying_users"),count("*").alias("transactions"))
)

gold_fitnesscenter_revenue = add_snapshot_columns(
    gold_fitnesscenter_revenue
)

write_gold_table(
    spark,
    gold_fitnesscenter_revenue,
    "gold_fitnesscenter_revenue",
    partition_cols=["date"]
)