from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("unit_price").alias("unit_price_count"))
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.channel"), F.col("O.unit_price_count"))
    .orderBy("unit_price_count")
)
