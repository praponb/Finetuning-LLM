from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.channel").alias("channel"),
        F.col("a.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.cost")).alias("min_cost"),
    )
)
