from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "outer")
    .filter(F.col("order_items.quantity").between(5, 7))
    .select(
        F.col("order_items.id"),
        F.col("order_items.quantity"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
    )
    .orderBy(F.col("city").desc())
)
