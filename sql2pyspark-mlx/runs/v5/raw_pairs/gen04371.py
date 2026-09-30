from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("orders.store_id"), "inner")
    .groupBy(F.col("orders.customer_id"))
    .agg(F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"))
    .orderBy(F.col("stddev_quantity").desc())
)
