from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("status") != "IN_TRANSIT").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.discount") < 0.38)
    .select(F.col("o.id"), F.col("o.shipping_fee"))
    .orderBy(F.col("shipping_fee").desc(), "id")
    .limit(3)
)
