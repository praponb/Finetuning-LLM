from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("orders.id") == F.col("payments.order_id"), "inner")
    .filter((F.col("orders.order_date") >= "2024-07-01") & (F.col("payments.paid_at") >= "2025-02-01"))
    .select(
        F.col("orders.order_date"),
        F.col("payments.paid_at"),
        F.col("payments.status"),
    )
)
