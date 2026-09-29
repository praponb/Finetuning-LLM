from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .filter(F.col("paid_at") >= "2025-04-01")
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("t"), F.col("o.id") == F.col("t.order_id"), "inner")
    .select(F.col("o.order_date"), F.col("t.max_amount"))
    .orderBy("order_date")
)
