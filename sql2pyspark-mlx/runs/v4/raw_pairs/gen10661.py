from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.count("*").alias("total_count"),
    )
)

result = (
    df_orders.alias("orders")
    .join(payments_by_order.alias("t"), F.col("orders.id") == F.col("t.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.status"),
        F.col("orders.amount"),
        F.col("t.amount_range"),
        F.col("t.total_count"),
    )
)
