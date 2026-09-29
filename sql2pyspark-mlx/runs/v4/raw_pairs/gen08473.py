from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.shipping_fee").isNotNull())
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(F.count(F.col("s.id")).alias("shipment_count"))
    .orderBy(F.col("id").desc(), "channel")
)
