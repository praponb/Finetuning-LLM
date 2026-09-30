from pyspark.sql import functions as F

order_summary = (
    df_payments
    .groupBy("order_id")
    .agg(F.countDistinct("amount").alias("unique_orders"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.unique_orders") > 6)
    .select(
        F.col("o.id"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("os.unique_orders"),
    )
)
