from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .filter(F.col("quantity") > 6)
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.store_id"),
        F.col("os.unique_orders"),
    )
)
