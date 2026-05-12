# utils/gold_utils.py

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, when
from delta.tables import DeltaTable


# -------------------------------
# DATA QUALITY
# -------------------------------

def assert_not_null(df: DataFrame, columns: list, table_name: str):

    for c in columns:
        if df.filter(col(c).isNull()).select(c).first():
            raise Exception(
                f"[DQ FAIL] {table_name}.{c} contains NULL"
            )


def assert_non_negative(df: DataFrame, column: str, table_name: str):

    if df.filter(col(column) < 0).select(column).first():
        raise Exception(
            f"[DQ FAIL] {table_name}.{column} negative"
        )


# -------------------------------
# AUDIT
# -------------------------------

def add_gold_audit(df: DataFrame, pipeline: str):

    return (
        df
        .withColumn("gold_ingest_time", current_timestamp())
        .withColumn("pipeline", lit(pipeline))
    )


# -------------------------------
# SAFE DIVIDE
# -------------------------------

def safe_divide(num, den):

    return when(den == 0, 0).otherwise(num / den)


# -------------------------------
# OVERWRITE (FOR KPI TABLES)
# -------------------------------

def write_gold_overwrite(
    df: DataFrame,
    output_path: str,
    partition_cols=None
):

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(output_path)


# -------------------------------
# UPSERT (FOR DIM TABLES)
# -------------------------------

def write_gold_upsert(
    spark,
    df: DataFrame,
    output_path: str,
    primary_key: str
):

    if not DeltaTable.isDeltaTable(spark, output_path):

        df.write.format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .save(output_path)

        return

    delta = DeltaTable.forPath(spark, output_path)

    (
        delta.alias("t")
        .merge(
            df.alias("s"),
            f"t.{primary_key} = s.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )