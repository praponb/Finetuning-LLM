from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .select(F.col("o.shipping_fee"), F.col("os.row_count"), F.col("os.last_paid_at"))
)
