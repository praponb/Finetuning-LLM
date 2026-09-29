from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "inner")
    .filter(F.col("pbo.max_amount") >= 1540)
    .select(F.col("o.channel"), F.col("o.order_date"), F.col("pbo.max_amount"))
    .orderBy(F.col("max_amount").desc(), "channel")
)
