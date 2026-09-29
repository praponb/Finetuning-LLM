from pyspark.sql import functions as F

order_summary = (
    df_shipments
    .groupBy("order_id")
    .agg(F.countDistinct("cost").alias("unique_orders"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(F.col("o.status"), F.col("o.shipping_fee"), F.col("O.unique_orders"))
    .orderBy("status")
)
