from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("shipment_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .filter(F.col("t.avg_cost") >= 78)
    .select(F.col("o.channel"), F.col("t.shipment_count"), F.col("t.avg_cost"))
    .orderBy(F.col("channel").desc())
)
