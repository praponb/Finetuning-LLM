from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w5 = Window.orderBy("quantity", "id")

with_windows = (
    df_inventory
    .select(
        "id",
        "product_id",
        "quantity",
        F.row_number().over(w).alias("event_seq"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
        F.dense_rank().over(w4).alias("quantity_dense_rank"),
        F.ntile(5).over(w5).alias("quantity_bucket"),
    )
)

valid = (
    with_windows
    .filter(F.col("quantity_bucket").isNull())
    .select(
        "*",
        (F.col("last_known_quantity") * F.col("cumulative_quantity")).alias("last_known_quantity_times_cumulative_quantity"),
    )
)

cleaned = (
    valid
    .filter(F.col("product_id").isin([1, 4, 5]))
    .select(
        "id",
        "quantity",
        "event_seq",
        "last_known_quantity",
        "cumulative_quantity",
        "quantity_dense_rank",
        "quantity_bucket",
        "last_known_quantity_times_cumulative_quantity",
        F.ceil("last_known_quantity_times_cumulative_quantity").alias("last_known_quantity_times_cumulative_quantity_whole"),
        (F.col("event_seq") / F.nullif("cumulative_quantity", F.lit(0))).alias("event_seq_per_cumulative_quantity"),
    )
)

base = (
    cleaned
    .filter(F.col("cumulative_quantity") > 1106)
    .select(
        "id",
        "event_seq",
        "last_known_quantity",
        "quantity_dense_rank",
        "quantity_bucket",
        "last_known_quantity_times_cumulative_quantity",
        "event_seq_per_cumulative_quantity",
        (F.col("quantity_bucket") / F.nullif("last_known_quantity_times_cumulative_quantity", F.lit(0))).alias("quantity_bucket_per_last_known_quantity_times_cumulative_quantity"),
        F.ceil("event_seq").alias("event_seq_whole"),
    )
)

totals = (
    base
    .rollup(
        F.when(F.col("last_known_quantity_times_cumulative_quantity") >= 67, "LARGE").otherwise("SMALL").alias("last_known_quantity_times_cumulative_quantity_tier"),
        F.when(F.col("quantity_bucket") >= 7, "LARGE").otherwise("SMALL").alias("quantity_bucket_tier"),
        F.when(F.col("event_seq_whole") >= 74, "PREMIUM").otherwise("STANDARD").alias("event_seq_whole_tier"),
    )
    .agg(
        F.min("last_known_quantity").alias("min_last_known_quantity"),
        F.min("quantity_bucket_per_last_known_quantity_times_cumulative_quantity").alias("min_quantity_bucket_per_last_known_quantity_times_cumulative_quantity"),
        F.percentile_approx("event_seq", 0.9).alias("event_seq_p90"),
    )
)

result = (
    totals
    .select(
        "quantity_bucket_tier",
        "event_seq_whole_tier",
        "min_last_known_quantity",
        "min_quantity_bucket_per_last_known_quantity_times_cumulative_quantity",
        "event_seq_p90",
    )
    .orderBy("min_last_known_quantity")
)
