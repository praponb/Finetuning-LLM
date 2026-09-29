from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
        F.sum(F.when(F.col("s.status") == "RETURNED", F.col("s.cost")).otherwise(0)).alias("returned_cost"),
    )
    .orderBy(F.col("shipment_count").desc())
)
