from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("quantity") < 4)
    .groupBy("order_id")
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.store_id"), F.col("o.channel"), F.col("t.avg_unit_price"))
    .orderBy("store_id")
)
