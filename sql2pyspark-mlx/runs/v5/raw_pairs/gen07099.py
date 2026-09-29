from pyspark.sql import functions as F

cleaned = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-09-06 13:00:00").cast("timestamp")) & F.col("method").isNotNull())
    .withColumn("amount_filled", F.coalesce("amount", F.lit(0)))
    .withColumn("status_method_key", F.concat_ws("-", "status", "method"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

result = (
    cleaned
    .groupBy("order_id")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.max("amount").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
