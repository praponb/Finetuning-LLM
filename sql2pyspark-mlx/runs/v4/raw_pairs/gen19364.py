from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("s.id")).alias("shipment_count"),
        F.count(F.when(F.col("s.carrier") == "Flash", 1)).alias("flash_count"),
    )
    .orderBy(F.col("flash_count").desc())
)
