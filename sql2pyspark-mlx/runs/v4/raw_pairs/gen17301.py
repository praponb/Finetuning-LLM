from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter(F.col("orders.amount") >= 1080)
    .select(
        F.col("orders.amount"),
        F.col("orders.store_id"),
        F.col("payments.order_id"),
        F.col("payments.paid_at"),
        F.col("payments.status"),
    )
)
