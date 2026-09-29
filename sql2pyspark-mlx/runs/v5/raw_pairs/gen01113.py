from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("os.last_paid_at"),
        F.col("os.amount_range"),
    )
    .orderBy(F.col("last_paid_at").desc())
)
