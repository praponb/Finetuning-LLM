from pyspark.sql import functions as F

order_totals = (
    df_payments
    .groupBy("order_id")
    .agg(F.sum(F.when(F.col("method") == "cash", 1).otherwise(0)).alias("cash_count"))
)

result = (
    df_orders.alias("o")
    .join(order_totals.alias("O"), F.col("o.id") == F.col("O.order_id"), "inner")
    .select(F.col("o.id"), F.col("o.shipping_fee"), F.col("O.cash_count"))
    .orderBy("cash_count")
)
