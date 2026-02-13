from pyspark.sql.functions import *

spark.sql("USE CATALOG fitnexus_cata")
spark.sql("USE SCHEMA gold")

users = spark.table("silver.user")
payments = spark.table("silver.payment")
bookings = spark.table("silver.booking")

user_bookings = (
    bookings
    .groupBy("userId")
    .agg(count("*").alias("total_bookings"))
)

user_revenue = (
    payments
    .filter(col("status")=="SUCCESS")
    .groupBy("userId")
    .agg(sum("amount").alias("total_spend"),count("*").alias("total_txn"))
)

gold_user_ltv = (
    users
    .join(user_revenue, users.id == user_revenue.userId, "left")
    .drop(user_revenue.userId)
    .join(user_bookings, users.id == user_bookings.userId, "left")
    .drop(user_bookings.userId)
    .fillna(0, ["total_spend", "total_bookings"])
)

gold_user_ltv = add_snapshot_columns(gold_user_ltv)

write_gold_table(spark, gold_user_ltv, "gold_user_lifetime_value")