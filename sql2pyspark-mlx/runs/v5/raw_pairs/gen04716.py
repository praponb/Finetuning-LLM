from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.card_count") > 45)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("os.card_count"),
        F.col("os.amount_count"),
    )
    .orderBy(F.col("store_id").desc())
)
