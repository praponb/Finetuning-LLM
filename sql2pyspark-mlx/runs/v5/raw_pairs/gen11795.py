from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.sum(F.col("s.cost")).alias("total_cost"),
    )
    .filter(F.col("total_cost") > 310)
    .orderBy(F.col("shipment_count").desc(), "total_cost")
)
