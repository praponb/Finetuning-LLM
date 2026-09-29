from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.amount"), F.col("o.order_date"), F.col("t.avg_cost"))
)
