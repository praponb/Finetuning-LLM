from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.revenue") >= 38)
    .select(
        F.col("o.discount"),
        F.col("o.customer_id"),
        F.col("os.revenue"),
        F.col("os.avg_unit_price"),
    )
    .orderBy(F.col("discount").desc())
)
