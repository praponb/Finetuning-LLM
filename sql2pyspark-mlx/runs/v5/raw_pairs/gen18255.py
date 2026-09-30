from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("payments.order_id"), "outer")
    .select(
        F.col("payments.id"),
        F.col("payments.order_id"),
        F.col("payments.status"),
        F.col("payments.paid_at"),
        F.col("orders.channel"),
        F.col("orders.store_id"),
        F.col("orders.shipping_fee"),
    )
)
