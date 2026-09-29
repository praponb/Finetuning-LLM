from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("orders")
    .join(order_totals.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(F.col("orders.id"), F.col("orders.status"), F.col("t.total_quantity"))
)
