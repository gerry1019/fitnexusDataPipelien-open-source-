from pyspark.sql.functions import *

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA gold")

fc = spark.table("silver.fitness_center")
subs = spark.table("silver.fitnesscentersubscription")
payments = spark.table("silver.payment")
users = spark.table("silver.user")
bookings = spark.table("silver.booking")

first_sub = subs.groupBy("fitnessCenterId").agg(
    min("createdAt").alias("first_subscription_date")
)

revenue = payments.filter(col("status") == "SUCCESS").groupBy(
    "fitnessCenterId"
).agg(sum("amount").alias("total_revenue"))

user_cnt = users.groupBy("fitnessCenterId").agg(count("*").alias("total_users"))

display(users)

# todo - when src db added fcId in booking table remove this join .
user_belong_to_fc = (
    bookings.alias("b")
    .join(
        users.alias("u"),
        col("b.userId") == col("u.id"),
        "left"
    )
    .select(
        col("b.*"),             # Selects all columns from the booking table
        col("u.fitnessCenterId") # Selects only the specific column from users
    )
)

booking_cnt = user_belong_to_fc.groupBy("fitnessCenterId").agg(
    count("*").alias("total_bookings")
)

fc_prepared = fc.withColumnRenamed("id", "fitnessCenterId")

gold_fc_lifecycle = (
    fc_prepared
    .join(first_sub, "fitnessCenterId", "left")
    .join(revenue, "fitnessCenterId", "left")
    .join(user_cnt, "fitnessCenterId", "left")
    .join(booking_cnt, "fitnessCenterId", "left")
    .withColumn(
        "days_since_login",
        when(col("lastLoginAt").isNull(), lit(999))
        .otherwise(datediff(current_date(), to_date("lastLoginAt")))
    )
    .withColumn(
        "lifecycle_stage",
        when(col("first_subscription_date").isNull(), "ONBOARDING")
        .when(col("days_since_login") > 60, "AT_RISK")
        .otherwise("ACTIVE")
    )
)

gold_fc_lifecycle = add_snapshot_columns(gold_fc_lifecycle)

display(gold_fc_lifecycle)

write_gold_table(spark, gold_fc_lifecycle, "gold_fitnesscenter_lifecycle")