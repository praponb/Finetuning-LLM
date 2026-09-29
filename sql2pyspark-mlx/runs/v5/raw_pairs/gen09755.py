from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.amount") > 1260) & F.col("order_items.unit_price").between(210, 230))
    .select(
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
        F.col("orders.status"),
        F.col("order_items.quantity"),
        F.col("order_items.order_id"),
    )
)
