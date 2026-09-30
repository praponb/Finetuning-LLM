from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.min(F.col("s.delivered_date")).alias("min_delivered_date"),
        F.avg(F.col("s.cost")).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 37)
)
