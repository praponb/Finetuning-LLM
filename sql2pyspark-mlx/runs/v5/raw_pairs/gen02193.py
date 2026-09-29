from pyspark.sql import functions as F

order_totals = (
    df_shipments
    .filter(F.col("cost").between(33, 66))
    .groupBy("order_id")
    .agg(F.max("delivered_date").alias("last_delivered_date"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.customer_id"), F.col("O.last_delivered_date"))
)
