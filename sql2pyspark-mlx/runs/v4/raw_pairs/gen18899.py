from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("line_total", "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_ordered = Window.partitionBy("order_id").orderBy("prev_line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("prev_line_total", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.orderBy("shipping_fee", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") - F.col("unit_price")))
)

metrics = (
    valid
    .withColumn("line_total_bucket", F.ntile(3).over(w))
    .withColumn("prev_line_total", F.lag("line_total", 2).over(w2))
)

joined = (
    metrics
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.shipping_fee"))
)

filtered = (
    joined
    .filter(F.col("order_id").isin([7, 25]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "line_total",
        "prev_line_total",
        "shipping_fee",
        F.greatest("shipping_fee", "quantity").alias("greatest_shipping_fee_quantity"),
    )
)

metrics_order_items = (
    filtered
    .withColumn("running_prev_line_total", F.sum("prev_line_total").over(w_ordered))
    .withColumn("greatest_shipping_fee_quantity_avg_2", F.avg("greatest_shipping_fee_quantity").over(w4))
    .withColumn("quantity_avg_5", F.avg("quantity").over(w_recent))
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w6))
)

with_dims = (
    metrics_order_items
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.channel"), F.col("o.discount"))
)

joined_order_items = (
    with_dims
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select(
        "s.*",
        F.col("p.order_date"),
        F.col("p.shipping_fee").alias("order_shipping_fee"),
        F.col("p.status"),
    )
)

result = (
    joined_order_items
    .groupBy(
        "channel",
        F.date_trunc("quarter", "order_date").alias("quarter_start"),
        "status",
    )
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_rows"),
    )
)
