from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .groupBy("order_id")
    .agg(F.sum("quantity").alias("total_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "left")
    .filter(F.col("ot.total_quantity") >= 26)
    .select(F.col("o.id"), F.col("o.channel"), F.col("ot.total_quantity"))
    .orderBy(F.col("total_quantity").desc())
)
