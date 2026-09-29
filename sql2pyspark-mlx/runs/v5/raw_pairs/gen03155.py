from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w_recent = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id")
w_hist = Window.partitionBy("order_id").orderBy("quantity", "id")
w_hist_2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w5 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("unit_price", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("order_id").orderBy("next_line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

metrics = (
    valid
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.max("quantity").over(w).alias("order_id_max_quantity"),
        F.lead("line_total", 1).over(w_recent).alias("next_line_total"),
    )
)

with_windows = (
    metrics
    .filter(F.col("order_id_max_quantity").isNotNull())
    .select(
        "*",
        F.row_number().over(w_hist).alias("seq_no"),
        F.ntile(10).over(w_hist_2).alias("unit_price_bucket"),
        F.last("order_id_max_quantity", ignorenulls=True).over(w5).alias("last_known_order_id_max_quantity"),
        F.ntile(5).over(w_hist_2).alias("unit_price_bucket_2"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.avg("unit_price_bucket").over(w6).alias("unit_price_bucket_avg_2_prior"),
        F.first("order_id_max_quantity", ignorenulls=True).over(w7).alias("first_known_order_id_max_quantity"),
    )
)

result = (
    scored
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel").alias("channel"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("e.order_id_max_quantity")), 2).alias("avg_order_id_max_quantity"),
    )
)
