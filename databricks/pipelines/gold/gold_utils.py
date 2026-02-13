from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from delta.tables import DeltaTable


def add_snapshot_columns(df: DataFrame) -> DataFrame:
    """Adds standard snapshot metadata to all gold tables"""
    return (
        df
        .withColumn("snapshot_date", current_date())
        .withColumn("snapshot_ts", current_timestamp())
    )


def write_gold_table(
        spark,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        partition_cols: list = None,
        primary_key: str = None  # Needed only for 'merge' mode
):
    """
    Standard writer for Gold tables.
    Supports:
    - 'overwrite': Standard full refresh.
    - 'merge': SCD Type 2 History tracking (Requires primary_key).
    """

    if df.isEmpty():
        print(f"⚠️ Skipping empty gold table write: {table_name}")
        return

    # --- OPTION 1: SCD TYPE 2 MERGE LOGIC ---
    if mode.lower() == "merge":
        if not primary_key:
            raise ValueError("❌ Primary Key is required for 'merge' mode!")

        # If table doesn't exist, create it fresh
        if not spark.catalog.tableExists(table_name):
            print(f"📦 Table {table_name} not found. Creating first version...")
            (df.withColumn("is_current", lit(True))
             .withColumn("effective_date", current_date())
             .withColumn("end_date", lit(None).cast("date"))
             .write.format("delta").mode("overwrite").saveAsTable(table_name))
            return

        # 1. Identify what has changed (Comparison)
        target_table = DeltaTable.forName(spark, table_name)

        # We join new data with target to find existing current records
        staged_updates = df.alias("updates").join(
            target_table.toDF().alias("target"),
            (col(f"updates.{primary_key}") == col(f"target.{primary_key}")) & (col("target.is_current") == True),
            "left_outer"
        ).selectExpr("updates.*", f"target.{primary_key} as target_pk")

        # 2. Execute Merge: Expire the old records
        (target_table.alias("t")
         .merge(staged_updates.alias("s"), f"t.{primary_key} = s.{primary_key}")
         .whenMatchedUpdate(
            condition="t.is_current = true",
            set={"is_current": "false", "end_date": "current_date()"}
        )
         .execute())

        # 3. Insert the new active records
        new_records = (staged_updates
                       .withColumn("is_current", lit(True))
                       .withColumn("effective_date", current_date())
                       .withColumn("end_date", lit(None).cast("date"))
                       .drop("target_pk"))

        new_records.write.format("delta").mode("append").saveAsTable(table_name)
        print(f"✅ SCD Type 2 Merge completed for {table_name}")

    # --- OPTION 2: STANDARD OVERWRITE / APPEND ---
    else:
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.saveAsTable(table_name)
        print(f"✅ Table {table_name} written successfully using mode: {mode}")