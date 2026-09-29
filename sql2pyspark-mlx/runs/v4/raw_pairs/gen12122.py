from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("oi.order_id").isin([8, 17]))
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .orderBy("status", F.col("id").desc())
    .limit(5)
)
