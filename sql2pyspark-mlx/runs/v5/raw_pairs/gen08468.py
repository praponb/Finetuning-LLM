from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.to_date("paid_at") < "2025-10-29")
    .groupBy("order_id")
    .agg(
        F.min("paid_at").alias("min_paid_at"),
        F.sum("amount").alias("total_amount"),
    )
)

result = (
    df_orders.alias("orders")
    .join(order_stats.alias("order_stats"), F.col("orders.id") == F.col("order_stats.order_id"), "left")
    .select(
        F.col("orders.id"),
        F.col("orders.order_date"),
        F.col("orders.status"),
        F.col("order_stats.min_paid_at"),
        F.col("order_stats.total_amount"),
    )
)
