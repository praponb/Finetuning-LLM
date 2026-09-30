from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("quantity").alias("quantity_count"))
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("t"), F.col("orders.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.quantity_count") > 7)
    .select(F.col("orders.id"), F.col("orders.status"), F.col("t.quantity_count"))
)
