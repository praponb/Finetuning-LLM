from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        "status",
        "method",
        F.when(F.col("amount") >= 1560, "high").when(F.col("amount") >= 220, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.count(F.when(F.col("status") == "SUCCESS", 1)).alias("success_count"),
    )
    .filter(F.col("max_amount") >= 970)
)
