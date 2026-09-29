from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("os.min_unit_price"),
        F.col("os.revenue"),
    )
    .orderBy("id", F.col("revenue").desc())
)
