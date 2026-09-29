from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.amount_range") >= 35)
    .select(F.col("o.discount"), F.col("o.status"), F.col("os.amount_range"))
)
