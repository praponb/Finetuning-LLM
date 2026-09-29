from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "right")
    .select(
        F.col("payments.order_id"),
        F.col("payments.status"),
        F.col("orders.amount"),
        F.col("orders.order_date"),
        F.col("orders.status").alias("order_status"),
        F.lower(F.col("orders.channel")).alias("channel_lower"),
    )
)
