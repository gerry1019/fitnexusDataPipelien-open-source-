from delta.tables import DeltaTable
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def run_silver_stream(
    df_stream,
    spark,
    checkpoint_path,
    silver_path,
    primary_key,
    updated_col
):

    (
        df_stream.writeStream
        .foreachBatch(lambda batch_df, batch_id:
            merge_microbatch(
                spark,
                batch_df,
                silver_path,
                primary_key,
                updated_col
            )
        )
        .outputMode("update")
        .option("checkpointLocation", checkpoint_path)
        .trigger(once=True)
        .start()
        .awaitTermination()
    )


def merge_microbatch(
    spark,
    batch_df,
    silver_path,
    primary_key,
    updated_col
):

    if batch_df.rdd.isEmpty():
        return

    batch_df = batch_df.filter(col(primary_key).isNotNull())

    window_spec = (
        Window
        .partitionBy(primary_key)
        .orderBy(col(updated_col).desc())
    )

    dedup_df = (
        batch_df
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # If table doesn't exist → create
    if not DeltaTable.isDeltaTable(spark, silver_path):

        (
            dedup_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(silver_path)
        )

        return

    delta_table = DeltaTable.forPath(spark, silver_path)

    (
        delta_table.alias("t")
        .merge(
            dedup_df.alias("s"),
            f"t.{primary_key} = s.{primary_key}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )