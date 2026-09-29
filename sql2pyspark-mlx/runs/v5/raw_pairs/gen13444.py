from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1370, "high").when(F.col("amount") >= 560, "mid").otherwise("low").alias("amount_bucket"),
        "method",
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count"),
        F.count("amount").alias("amount_count"),
    )
    .filter(F.col("avg_amount") >= 1510)
    .orderBy(F.col("avg_amount").desc(), F.col("amount_count").desc())
)
