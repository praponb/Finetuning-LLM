from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.avg("amount").alias("avg_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(payments_by_order.alias("payments_by_order"), F.col("orders.id") == F.col("payments_by_order.order_id"), "inner")
    .select(
        F.col("orders.status"),
        F.col("orders.customer_id"),
        F.col("payments_by_order.amount_range"),
        F.col("payments_by_order.avg_amount"),
    )
    .orderBy(F.col("status").desc(), "amount_range")
)
