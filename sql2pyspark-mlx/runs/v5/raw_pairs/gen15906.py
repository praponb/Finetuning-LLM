from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "right")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "left")
    .groupBy(F.col("orders.status"))
    .agg(F.round(F.avg(F.col("order_items.unit_price")), 2).alias("avg_unit_price"))
)
