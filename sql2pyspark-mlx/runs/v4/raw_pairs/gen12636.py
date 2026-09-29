from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .groupBy("order_id")
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
)

result = (
    df_orders.alias("orders")
    .join(order_items_by_order.alias("order_items_by_order"), F.col("orders.id") == F.col("order_items_by_order.order_id"), "inner")
    .select(
        F.col("orders.id"),
        F.col("orders.amount"),
        F.col("order_items_by_order.stddev_unit_price"),
    )
    .orderBy("stddev_unit_price")
)
