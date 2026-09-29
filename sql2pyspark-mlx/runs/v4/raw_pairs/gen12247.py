from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.min(F.col("s.cost")).alias("min_cost"),
    )
    .orderBy(F.col("shipment_count").desc())
)
