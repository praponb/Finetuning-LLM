from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("order_items.order_id"),
        F.col("orders.order_date"),
        F.col("orders.store_id"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
    )
)
