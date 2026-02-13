%run /Workspace/Users/anuragya8362@gmail.com/fitnexus-project/gold-transformations/gold_utils.py

from pyspark.sql.functions import *

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA gold")

session_df = spark.table("silver.session")
booking_df = spark.table("silver.booking")

booking_count_df = (
    booking_df
    .groupBy("sessionId")
    .agg(count("id").alias("booked_count"))
)

gold_session_utilization = (
    session_df
    .join(booking_count_df, session_df.id == booking_count_df.sessionId, "left")
    .withColumn("booked_count", coalesce(col("booked_count"), lit(0)))
    .withColumn(
        "utilization_percentage",
        when(
            col("availableSlots").isNull() | (col("availableSlots") == 0),
            lit(0.0)
        ).otherwise(
            round((col("booked_count") / col("availableSlots")) * 100, 2)
        )
    )
    .select(
        col("id").alias("session_id"),
        "fitnessCenterId",
        "startTime",
        "endTime",
        "availableSlots",
        "booked_count",
        "utilization_percentage"
    )
)

gold_session_utilization = add_snapshot_columns(gold_session_utilization)

write_gold_table(spark, gold_session_utilization, "gold_session_utilization")
