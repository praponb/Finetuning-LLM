from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("orders.store_id").isin([19, 22]))
    .select(
        F.col("payments.id"),
        F.col("payments.amount"),
        F.col("payments.order_id"),
        F.col("payments.paid_at"),
        F.col("orders.discount"),
        F.col("orders.order_date"),
        F.col("orders.amount").alias("order_amount"),
    )
)
