from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.min(F.col("s.cost")).alias("min_cost"),
        F.min(F.col("s.delivered_date")).alias("first_delivered_date"),
    )
    .filter(F.col("shipment_count") > 1)
    .orderBy("status", F.col("shipment_count").desc())
    .limit(1)
)
