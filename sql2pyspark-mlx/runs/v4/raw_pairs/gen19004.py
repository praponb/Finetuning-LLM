from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.avg("cost").alias("avg_cost"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.channel"), F.col("O.avg_cost"))
    .orderBy(F.col("id").desc())
)
