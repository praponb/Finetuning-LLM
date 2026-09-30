from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left")
    .groupBy(F.col("o.status"))
    .agg(F.count(F.col("s.id")).alias("shipment_count"))
    .orderBy(F.col("shipment_count").desc(), F.col("status").desc())
)
