from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-04-01")
    .groupBy(
        F.when(F.col("amount") >= 1290, "high").when(F.col("amount") >= 630, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
)
