from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.channel"), F.col("t.avg_cost"))
)
