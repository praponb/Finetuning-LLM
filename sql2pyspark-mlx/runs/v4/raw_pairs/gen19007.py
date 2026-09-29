from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity_bucket").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity_bucket").asc_nulls_last())
w5 = Window.partitionBy("store_id")
w_running = Window.partitionBy("store_id").orderBy(F.col("row_seq").desc())
w7 = Window.partitionBy("product_id", "store_id").orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)

metrics = (
    df_inventory
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(5).over(w2).alias("quantity_bucket"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_windows = (
    latest
    .select(
        "*",
        F.percent_rank().over(w4).alias("quantity_bucket_percent_rank"),
        F.round((F.col("quantity_bucket") / F.sum("quantity_bucket").over(w5)), 4).alias("quantity_bucket_share"),
        F.dense_rank().over(w_running).alias("row_seq_dense_rank"),
        F.min("quantity").over(w5).alias("store_id_min_quantity"),
        F.avg("quantity").over(w7).alias("quantity_avg_5"),
    )
)

result = (
    with_windows
    .groupBy(
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
        F.when(F.col("quantity_bucket_share") >= 0.89, "LARGE").otherwise("SMALL").alias("quantity_bucket_share_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity_avg_5", [0.25, 0.5, 0.75]).alias("quantity_avg_5_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.percentile_approx("quantity_avg_5", 0.9).alias("quantity_avg_5_p90"),
    )
    .orderBy(F.col("quantity_avg_5_p90").desc())
)
