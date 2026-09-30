from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity").between(6, 8))
    .groupBy("order_id")
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "inner")
    .select(
        F.col("orders.discount"),
        F.col("orders.store_id"),
        F.col("order_stats.quantity_count"),
        F.col("order_stats.max_quantity"),
    )
    .orderBy("discount")
)
