from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("method").isin(["cash", "paypal"]))
    .groupBy("order_id")
    .agg(
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.min_amount") > 540)
    .select(
        F.col("o.customer_id"),
        F.col("o.channel"),
        F.col("os.max_amount"),
        F.col("os.min_amount"),
    )
    .orderBy(F.col("customer_id").desc())
)
