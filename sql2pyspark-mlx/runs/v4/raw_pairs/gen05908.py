from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.round(F.stddev(F.col("s.cost")), 2).alias("stddev_cost"),
    )
)
