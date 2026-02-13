from delta.tables import DeltaTable

def run_silver_stream(df_stream, spark, checkpoint_path, silver_table, primary_key, updated_col):

    df_stream.writeStream
      .foreachBatch(lambda batch_df, batch_id:
          merge_microbatch(
              spark,
              batch_df,
              silver_table,
              primary_key,
              updated_col
          )
      )
      .outputMode("update")
      .option("checkpointLocation", checkpoint_path)
      .trigger(once=True)
      .start()


def merge_microbatch(
    spark,
    batch_df,
    silver_table,
    primary_key,
    updated_col
):
    # skip empty batches
    if batch_df.isEmpty():
        return

    # Ensure primary key exists
    batch_df = batch_df.filter(col(primary_key).isNotNull())

    # Deduplicate within microbatch
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

    # 🔹 IF TABLE DOES NOT EXIST → CREATE IT
    if not spark.catalog.tableExists(silver_table):
        (
            dedup_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
        )
        return

    # 🔹 ELSE → MERGE
    delta_table = DeltaTable.forName(spark, silver_table)

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
