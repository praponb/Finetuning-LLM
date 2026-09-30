from pyspark.sql import functions as F

order_totals = (
    df_order_items
    .filter(F.col("order_id").isin([14, 25]))
    .groupBy("order_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("o.order_date"), F.col("t.avg_quantity"))
    .orderBy(F.col("order_date").desc(), F.col("avg_quantity").desc())
)
