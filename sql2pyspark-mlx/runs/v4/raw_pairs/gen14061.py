from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.status"),
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("p.method"),
        F.col("p.status").alias("payment_status"),
    )
)
