from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("method").isin(["card", "promptpay"]))
    .groupBy("order_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.amount"),
        F.col("t.avg_amount"),
    )
    .orderBy("amount", "id")
    .limit(50)
)
