from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .groupBy(F.col("o.status"))
    .agg(F.count(F.col("oi.id")).alias("order_item_count"))
    .orderBy("status")
    .limit(50)
)
