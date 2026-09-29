from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.discount").between(0.14, 0.39))
    .select(F.col("o.id"), F.col("o.status"), F.col("o.channel"))
    .orderBy("status", "channel")
)
