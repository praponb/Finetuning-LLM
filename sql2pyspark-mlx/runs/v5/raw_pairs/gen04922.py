from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc())
w2 = Window.partitionBy("order_id", "channel").orderBy("quantity", "id")
w3 = Window.partitionBy("channel", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id", "product_id").orderBy(F.col("quantity").desc(), "id")

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([14, 26]))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

joined = (
    filtered
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.channel"))
)

enriched = (
    joined
    .select(
        "*",
        F.cume_dist().over(w).alias("quantity_filled_cume_dist"),
        F.ntile(10).over(w2).alias("quantity_bucket"),
        F.sum("quantity_filled").over(w3).alias("cumulative_quantity_filled"),
        F.first("quantity", ignorenulls=True).over(w4).alias("first_known_quantity"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

base = (
    enriched
    .filter(F.col("product_id").isNotNull() & (~F.col("channel").eqNullSafe(F.lit("web"))))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "channel",
        "quantity_filled_cume_dist",
        "quantity_bucket",
        "cumulative_quantity_filled",
        "event_seq",
        F.ceil("quantity_filled_cume_dist").alias("quantity_filled_cume_dist_whole"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("quantity_bucket") >= 7, "LARGE").otherwise("SMALL").alias("quantity_bucket_tier"),
    )
    .agg(
        F.sum("quantity_bucket").alias("total_quantity_bucket"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity_filled_cume_dist_whole", [0.25, 0.5, 0.75]).alias("quantity_filled_cume_dist_whole_quartiles"),
    )
)

result = (
    summary
    .select(
        "quantity_bucket_tier",
        "quantity_bit_or",
        "row_count",
        "quantity_filled_cume_dist_whole_quartiles",
    )
    .orderBy("quantity_bucket_tier", F.col("row_count").desc())
)
