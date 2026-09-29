from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("paid_at").alias("min_paid_at"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.order_date"), F.col("os.min_paid_at"))
    .orderBy(F.col("id").desc(), "order_date")
    .limit(10)
)
