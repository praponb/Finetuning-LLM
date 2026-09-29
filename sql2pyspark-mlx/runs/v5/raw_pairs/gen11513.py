from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("order_id") == 16)
    .groupBy("order_id")
    .agg(F.sum("cost").alias("total_cost"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(F.col("orders.discount"), F.col("t.total_cost"))
    .orderBy(F.col("total_cost").desc())
)
