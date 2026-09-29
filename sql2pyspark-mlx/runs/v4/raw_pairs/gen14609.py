from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-29").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("is_card", F.when(F.col("method") == "card", 1).otherwise(0))
    .withColumn("method_filled", F.coalesce("method", F.lit("NONE")))
)

result = (
    base
    .groupBy("method")
    .pivot("status", ["FAILED", "REFUNDED"])
    .agg(F.avg("is_card").alias("total"), F.count(F.lit(1)).alias("n"))
)
