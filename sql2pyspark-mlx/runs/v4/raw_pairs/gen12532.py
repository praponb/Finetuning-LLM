from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "FAILED"]))
    .groupBy("order_id")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .select(
        F.col("o.shipping_fee"),
        F.col("o.channel"),
        F.col("os.latest_paid_at"),
        F.col("os.success_count"),
    )
    .orderBy("shipping_fee", F.col("success_count").desc())
)
