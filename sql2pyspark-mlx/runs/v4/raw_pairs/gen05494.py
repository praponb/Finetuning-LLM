from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .groupBy(
        F.when(F.col("amount") >= 1350, "high").when(F.col("amount") >= 580, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"))
    .filter(F.col("failed_count") >= 5)
    .orderBy(F.col("amount_bucket").desc(), F.col("failed_count").desc())
)
