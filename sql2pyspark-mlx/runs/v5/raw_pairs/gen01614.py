from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .filter(F.col("order_item_count") >= 1)
    .orderBy(F.col("id").desc())
)
