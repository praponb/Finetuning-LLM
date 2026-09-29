from pyspark.sql import functions as F

order_stats = (
    df_payments
    .filter(F.col("order_id") == 36)
    .groupBy("order_id")
    .agg(F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("os"), F.col("o.id") == F.col("os.order_id"), "inner")
    .filter(F.col("os.success_count") > 9)
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.channel"),
        F.col("os.success_count"),
    )
    .orderBy(F.col("success_count").desc(), "id")
    .limit(3)
)
