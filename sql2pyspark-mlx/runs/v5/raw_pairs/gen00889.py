from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc_nulls_last())
w2 = Window.orderBy("quantity_filled")
w3 = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("product_id", "order_id")
w6 = Window.orderBy("quantity_filled_dense_rank", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy("line_total", "id")
w8 = Window.orderBy(F.col("line_total_bucket").desc(), "id")
w_part = Window.orderBy(F.col("line_total_dense_rank").desc(), "id")
w10 = Window.orderBy(F.col("line_total_bucket_bucket").desc_nulls_last())
w11 = Window.orderBy("line_total_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 15))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

metrics = (
    base
    .select(
        "id",
        "quantity",
        "line_total",
        F.dense_rank().over(w).alias("line_total_dense_rank"),
        F.dense_rank().over(w2).alias("quantity_filled_dense_rank"),
        F.ntile(4).over(w3).alias("line_total_bucket"),
        F.avg("line_total").over(w4).alias("line_total_avg_5"),
        F.count("quantity_filled").over(w5).alias("product_id_count_quantity_filled"),
    )
)

filtered = (
    metrics
    .filter(F.col("line_total_avg_5").isNotNull())
    .select(
        "id",
        "line_total",
        "line_total_dense_rank",
        "quantity_filled_dense_rank",
        "line_total_bucket",
        "line_total_avg_5",
        "product_id_count_quantity_filled",
        F.round((F.col("product_id_count_quantity_filled") * 1.1), 2).alias("adjusted_product_id_count_quantity_filled"),
    )
)

enriched = (
    filtered
    .withColumn("line_total_dense_rank_avg_3", F.avg("line_total_dense_rank").over(w6))
    .withColumn("next_line_total_avg_5", F.lead("line_total_avg_5", 1).over(w7))
    .withColumn("line_total_bucket_bucket", F.ntile(4).over(w8))
)

with_windows = (
    enriched
    .select(
        "id",
        "line_total",
        "line_total_bucket",
        "line_total_dense_rank_avg_3",
        "next_line_total_avg_5",
        F.row_number().over(w_part).alias("event_seq"),
        F.rank().over(w10).alias("line_total_bucket_bucket_rank"),
        F.last("line_total", ignorenulls=True).over(w11).alias("last_known_line_total"),
    )
)

base_order_items = (
    with_windows
    .filter(F.col("next_line_total_avg_5") != 38)
    .select(
        "id",
        "line_total",
        "line_total_bucket",
        "next_line_total_avg_5",
        "event_seq",
        "line_total_bucket_bucket_rank",
        "last_known_line_total",
        F.round((F.col("last_known_line_total") * 1.15), 2).alias("last_known_line_total_scaled"),
    )
)

cleaned = (
    base_order_items
    .filter(F.col("event_seq").between(4, 5))
    .withColumn("line_total_bucket_bucket_rank_filled", F.coalesce("line_total_bucket_bucket_rank", F.lit(0)))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("last_known_line_total_scaled") >= 61, "PREMIUM").otherwise("STANDARD").alias("last_known_line_total_scaled_tier"),
        F.when(F.col("next_line_total_avg_5") >= 62, "PREMIUM").otherwise("STANDARD").alias("next_line_total_avg_5_tier"),
        F.when(F.col("line_total_bucket_bucket_rank") >= 8, "HIGH").otherwise("LOW").alias("line_total_bucket_bucket_rank_tier"),
    )
    .agg(
        F.percentile_approx("line_total_bucket_bucket_rank", [0.25, 0.5, 0.75]).alias("line_total_bucket_bucket_rank_quartiles"),
        F.round(F.avg("last_known_line_total"), 2).alias("avg_last_known_line_total"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("avg_last_known_line_total") >= 0)
    .orderBy(F.col("line_total_bucket_bucket_rank_tier").desc())
)
