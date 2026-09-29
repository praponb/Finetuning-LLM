from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .groupBy(
        F.when(F.col("amount") >= 1560, "high").when(F.col("amount") >= 460, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") > 1110)
    .select("amount_bucket", "method", "failed_count", "min_amount")
    .orderBy("amount_bucket")
)
