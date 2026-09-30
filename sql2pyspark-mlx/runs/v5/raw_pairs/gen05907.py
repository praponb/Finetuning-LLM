from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.count("*").alias("cnt"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.cnt") >= 5)
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("os.cnt"),
        F.col("os.max_quantity"),
    )
    .orderBy(F.col("max_quantity").desc())
)
