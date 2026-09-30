from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isin(["promptpay", "card"]))
    .groupBy(
        "method",
        "status",
        F.when(F.col("amount") >= 1460, "high").when(F.col("amount") >= 450, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("avg_amount") >= 26)
)
