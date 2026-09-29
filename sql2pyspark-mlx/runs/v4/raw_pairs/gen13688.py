from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .filter(F.col("pbo.refunded_count") > 5)
    .select(F.col("o.channel"), F.col("pbo.refunded_count"), F.col("pbo.last_paid_at"))
)
