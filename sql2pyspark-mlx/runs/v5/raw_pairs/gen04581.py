from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("product_id").isin([9, 34]))
    .groupBy("order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unit_price_count") >= 10)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("os.unit_price_count"),
        F.col("os.revenue"),
    )
    .orderBy(F.col("customer_id").desc())
)
