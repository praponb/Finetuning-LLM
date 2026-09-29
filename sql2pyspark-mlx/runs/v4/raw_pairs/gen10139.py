from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_payments.alias("payments"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("payments.order_id").isin([17, 28, 34, 39]) & (F.col("orders.discount") >= 0.29))
    .select(
        F.col("orders.customer_id"),
        F.col("orders.store_id"),
        F.col("orders.channel"),
        F.col("payments.order_id"),
        F.col("payments.paid_at"),
    )
)
