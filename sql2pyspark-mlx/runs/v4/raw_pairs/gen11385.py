from pyspark.sql import functions as F

order_items_by_order = (
    df_order_items
    .filter(F.col("quantity").between(4, 5))
    .groupBy("order_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_items_by_order.alias("oibo"), F.col("o.id") == F.col("oibo.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.shipping_fee"),
        F.col("oibo.max_unit_price"),
        F.col("oibo.quantity_range"),
    )
)
