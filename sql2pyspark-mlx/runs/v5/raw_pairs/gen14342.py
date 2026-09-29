from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("order_items.unit_price") >= 250)
    .select(
        F.col("order_items.order_id"),
        F.col("order_items.quantity"),
        F.col("order_items.unit_price"),
        F.col("orders.shipping_fee"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
    )
    .orderBy(F.col("store_id").desc())
)
