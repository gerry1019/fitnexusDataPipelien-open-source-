from pyspark.sql.functions import *
from utils.spark_utils import create_spark_session
from utils.silver_utils import run_silver_stream
from config.s3_config import S3_CONFIG

PRIMARY_KEY = "id"
UPDATED_COL = "updatedAt"

bucket = S3_CONFIG["bucket"]

BRONZE_PATH = f"s3a://{bucket}/bronze/FitnessCenterMembershipPlan"
SILVER_PATH = f"s3a://{bucket}/silver/fitness_center_membership_plan"
CHECKPOINT_PATH = f"s3a://{bucket}/silver/fitness_center_membership_plan/_checkpoint"

spark = create_spark_session("Silver FitnessCenterMembershipPlan")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)

df_clean = (
    df_stream
    .withColumn("name", initcap(trim(col("name"))))
    .withColumn("status", upper(trim(col("status"))))
    .withColumn("price", col("price").cast("double"))
    .withColumn("durationDays", col("durationDays").cast("int"))
.withColumn(
    "benefits",
    transform(col("benefits"), lambda x: trim(x))
    )
    .withColumn("createdAt", to_timestamp("createdAt"))
    .withColumn("updatedAt", to_timestamp("updatedAt"))
    .filter(col(UPDATED_COL).isNotNull())
    .filter(col(PRIMARY_KEY).isNotNull())
)

run_silver_stream(df_clean, spark, CHECKPOINT_PATH, SILVER_PATH, PRIMARY_KEY, UPDATED_COL)
spark.stop()