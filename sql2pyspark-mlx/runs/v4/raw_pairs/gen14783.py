from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.shipping_fee"),
        F.col("o.status"),
        F.col("os.min_amount"),
    )
)
