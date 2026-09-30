from pyspark.sql import functions as F

order_totals = (
    df_payments
    .filter(F.col("paid_at") < "2024-02-01")
    .groupBy("order_id")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("ot.avg_amount"),
        F.col("ot.first_paid_at"),
    )
    .orderBy(F.col("id").desc(), "first_paid_at")
)
