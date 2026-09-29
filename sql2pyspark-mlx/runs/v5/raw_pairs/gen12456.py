from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .select(F.col("o.channel"), F.col("o.customer_id"), F.col("sbo.avg_cost"))
)
