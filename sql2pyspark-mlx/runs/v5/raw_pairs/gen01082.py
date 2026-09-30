from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .filter(F.col("sbo.avg_cost") > 52)
    .select(F.col("o.discount"), F.col("sbo.min_delivered_date"), F.col("sbo.avg_cost"))
)
