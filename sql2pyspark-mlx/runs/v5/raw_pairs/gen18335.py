from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("oi.unit_price") >= 210)
    .groupBy(F.col("o.channel"), F.col("o.status"))
    .agg(F.max(F.col("oi.quantity")).alias("max_quantity"))
    .orderBy("status", "channel")
)
