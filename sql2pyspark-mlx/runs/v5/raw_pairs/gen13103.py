from pyspark.sql import functions as F

shipments_by_order = (
    df_shipments
    .groupBy("order_id")
    .agg(F.min("cost").alias("min_cost"))
)

result = (
    df_orders.alias("o")
    .join(shipments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(F.col("o.id"), F.col("o.amount"), F.col("o.channel"), F.col("t.min_cost"))
    .orderBy(F.col("amount").desc())
)
