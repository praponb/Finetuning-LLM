from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price_filled", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("next_unit_price_filled", "id").rowsBetween(-5, -1)
w5 = Window.orderBy("unit_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy("line_total", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") == 7))
    .select(
        "*",
        (F.col("quantity") - F.col("unit_price")).alias("line_total"),
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.lead("unit_price_filled").over(w).alias("next_unit_price_filled"),
        F.lag("unit_price_filled").over(w).alias("prev_unit_price_filled"),
        F.lag("unit_price").over(w).alias("prev_unit_price"),
        F.row_number().over(w).alias("seq_no"),
    )
)

with_windows = (
    metrics
    .withColumn("last4_sum_unit_price", F.sum("unit_price").over(w2))
    .withColumn("last4_count_unit_price", F.count("unit_price").over(w2))
    .withColumn("last4_avg_unit_price", F.avg("unit_price").over(w2))
    .withColumn("running_unit_price_filled", F.sum("unit_price_filled").over(w3))
)

enriched = (
    with_windows
    .filter(F.col("last4_avg_unit_price") > 170)
    .select(
        "id",
        "quantity",
        "unit_price",
        "prev_unit_price_filled",
        "running_unit_price_filled",
        F.avg("quantity").over(w_hist).alias("quantity_avg_5_prior"),
        F.sum("prev_unit_price").over(w5).alias("cumulative_prev_unit_price"),
        F.lead("quantity", 1).over(w_part).alias("next_quantity"),
    )
)

result = (
    enriched
    .select(
        "id",
        "quantity",
        "unit_price",
        "prev_unit_price_filled",
        "quantity_avg_5_prior",
        "cumulative_prev_unit_price",
    )
    .orderBy(F.col("quantity_avg_5_prior").asc_nulls_first())
)
