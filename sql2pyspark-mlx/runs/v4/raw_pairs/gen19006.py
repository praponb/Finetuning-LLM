from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("status") == "LOST").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .select(F.col("o.id"), F.col("o.shipping_fee"))
    .orderBy(F.col("id").desc())
)
