from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("cumulative_unit_price", "id")
w5 = Window.partitionBy("order_id").orderBy("corr_unit_price_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("product_id_min_quantity", "id")
w_recent = Window.partitionBy("order_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("order_id").orderBy(F.col("order_id_sum_corr_unit_price_quantity").asc_nulls_first())

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([3, 28, 31]))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 1.07), 2))
)

metrics = (
    filtered
    .select(
        "id",
        "order_id",
        "line_total",
        F.sum("unit_price").over(w).alias("cumulative_unit_price"),
        F.min("quantity").over(w2).alias("product_id_min_quantity"),
        F.corr("unit_price", "quantity").over(w3).alias("corr_unit_price_quantity"),
    )
)

with_windows = (
    metrics
    .withColumn("next_cumulative_unit_price", F.lead("cumulative_unit_price", 1).over(w4))
    .withColumn("cumulative_line_total", F.sum("line_total").over(w5))
    .withColumn("order_id_sum_corr_unit_price_quantity", F.sum("corr_unit_price_quantity").over(w3))
    .withColumn("event_seq", F.row_number().over(w6))
)

with_dims = (
    with_windows
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.line_total",
        "e.cumulative_line_total",
        "e.order_id_sum_corr_unit_price_quantity",
        "e.event_seq",
        F.col("o.amount"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "line_total",
        "cumulative_line_total",
        "event_seq",
        F.count("cumulative_line_total").over(w_recent).alias("last4_count_cumulative_line_total"),
        F.avg("cumulative_line_total").over(w_recent).alias("last4_avg_cumulative_line_total"),
        F.max("cumulative_line_total").over(w_recent).alias("last4_max_cumulative_line_total"),
        F.dense_rank().over(w8).alias("order_id_sum_corr_unit_price_quantity_dense_rank"),
    )
)
