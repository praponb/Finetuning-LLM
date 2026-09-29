from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("amount") <= 1180)
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("method") == "card", 1)).alias("card_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("O"), F.col("o.id") == F.col("O.order_id"), "left")
    .select(F.col("o.customer_id"), F.col("o.channel"), F.col("O.card_count"))
)
