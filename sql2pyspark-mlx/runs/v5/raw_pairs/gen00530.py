from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([17, 21, 28, 38]))
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.min(F.col("s.cost")).alias("min_cost"),
        F.avg(F.col("s.cost")).alias("avg_cost"),
    )
    .filter(F.col("avg_cost") >= 48)
    .orderBy("min_cost")
)
