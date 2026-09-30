from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_order_items.filter(F.col("unit_price") <= 150).alias("order_items"), F.col("orders.id") == F.col("order_items.order_id"), "left_semi")
    .filter(F.col("orders.customer_id").isNotNull())
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.store_id"),
        F.col("orders.customer_id"),
    )
)
