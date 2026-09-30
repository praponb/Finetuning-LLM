from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("unit_price") >= 200)
    .groupBy("order_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("OrderStats"), F.col("orders.id") == F.col("OrderStats.order_id"), "inner")
    .filter(F.col("OrderStats.avg_unit_price") >= 76)
    .select(
        F.col("orders.id"),
        F.col("orders.shipping_fee"),
        F.col("OrderStats.total_quantity"),
        F.col("OrderStats.avg_unit_price"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
