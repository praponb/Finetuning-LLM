from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter((F.col("orders.channel") == "partner") & (F.col("payments.method") == "card"))
    .select(
        F.col("payments.id"),
        F.col("payments.status"),
        F.col("payments.order_id"),
        F.col("orders.customer_id"),
        F.col("orders.status").alias("order_status"),
    )
)
