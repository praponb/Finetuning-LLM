from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("dim"), F.col("s.order_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
        F.col("s.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("s.cost")), 2).alias("avg_cost"),
    )
)
