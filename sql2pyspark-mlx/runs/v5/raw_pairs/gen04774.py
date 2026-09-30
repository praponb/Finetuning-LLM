from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee") >= 16)
    .select(
        F.col("o.discount"),
        F.col("o.status"),
        F.col("p.status").alias("payment_status"),
        F.col("p.order_id"),
    )
)
