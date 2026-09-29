from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .select(
        F.col("orders.store_id"),
        F.col("order_items.unit_price"),
        (F.col("order_items.unit_price") - F.col("order_items.quantity")).alias("line_total"),
    )
)
