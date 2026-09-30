from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("sbo"), F.col("o.id") == F.col("sbo.order_id"), "left")
    .filter(F.col("sbo.total_cost") >= 310)
    .select(F.col("o.shipping_fee"), F.col("sbo.total_cost"))
    .orderBy("shipping_fee", F.col("total_cost").desc())
)
