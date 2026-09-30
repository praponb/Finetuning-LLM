from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.amount_count") >= 6)
    .select(F.col("o.discount"), F.col("os.amount_count"))
    .orderBy(F.col("amount_count").desc(), F.col("discount").desc())
)
