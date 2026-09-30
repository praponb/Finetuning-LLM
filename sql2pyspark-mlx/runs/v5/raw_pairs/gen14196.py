from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .groupBy("order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .select(F.col("o.store_id"), F.col("pbo.amount_range"), F.col("pbo.last_paid_at"))
)
