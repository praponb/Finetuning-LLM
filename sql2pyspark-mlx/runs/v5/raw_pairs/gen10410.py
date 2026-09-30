from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .groupBy("order_id")
    .agg(F.avg("unit_price").alias("avg_unit_price"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.store_id"), F.col("os.avg_unit_price"))
    .orderBy(F.col("store_id").desc())
)
