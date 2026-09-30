from pyspark.sql import functions as F

order_summary = (
    df_payments
    .filter(F.col("amount") >= 940)
    .groupBy("order_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("t.avg_amount"),
    )
    .orderBy("amount")
)
