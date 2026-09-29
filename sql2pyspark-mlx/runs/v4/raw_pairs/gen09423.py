from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.channel"),
        F.col("o.discount"),
        F.col("os.max_amount"),
        F.col("os.amount_count"),
    )
)
