from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .groupBy("order_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.discount"), F.col("o.channel"), F.col("O.cnt"))
)
