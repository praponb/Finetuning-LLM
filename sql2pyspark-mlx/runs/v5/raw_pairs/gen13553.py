from pyspark.sql import functions as F

order_stats = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.max("paid_at").alias("latest_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.refunded_count") > 4)
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("os.refunded_count"),
        F.col("os.latest_paid_at"),
    )
    .orderBy(F.col("store_id").desc(), "id")
    .limit(3)
)
