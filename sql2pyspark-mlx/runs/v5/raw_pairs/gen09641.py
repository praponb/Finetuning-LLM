from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .groupBy("order_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("P"), F.col("o.id") == F.col("P.order_id"), "inner")
    .select(F.col("o.status"), F.col("P.amount_count"))
)
