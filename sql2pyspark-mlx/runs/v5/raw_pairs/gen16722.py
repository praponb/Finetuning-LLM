from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.status"),
        F.col("payments.paid_at"),
        F.col("orders.status").alias("order_status"),
        F.col("orders.customer_id"),
        F.col("orders.shipping_fee"),
        F.lower(F.col("payments.status")).alias("status_lower"),
    )
)
