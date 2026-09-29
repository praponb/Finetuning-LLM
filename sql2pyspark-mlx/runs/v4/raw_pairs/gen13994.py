from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.count("*").alias("order_item_count"))
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("order_items_by_order"), F.col("orders.id") == F.col("order_items_by_order.order_id"), "left")
    .filter(F.col("order_items_by_order.order_item_count") >= 10)
    .select(
        F.col("orders.id"),
        F.col("orders.store_id"),
        F.col("orders.order_date"),
        F.col("order_items_by_order.order_item_count"),
    )
)
