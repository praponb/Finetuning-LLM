from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.min("amount").alias("min_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("ot"), F.col("o.id") == F.col("ot.order_id"), "inner")
    .select(F.col("o.customer_id"), F.col("o.discount"), F.col("ot.min_amount"))
    .orderBy("customer_id")
)
