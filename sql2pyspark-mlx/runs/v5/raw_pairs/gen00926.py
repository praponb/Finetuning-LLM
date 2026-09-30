from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.alias("order_items"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .select(
        F.col("orders.amount"),
        F.col("orders.shipping_fee"),
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
    )
    .orderBy("quantity")
)
