from pyspark.sql import functions as F

filtered_payments = df_payments.filter(F.col("status").isin(["FAILED", "SUCCESS", "REFUNDED"]))

summary = (
    filtered_payments
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1750, "high").when(F.col("amount") >= 750, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    summary
    .select("status", "amount_bucket", "stddev_amount", "min_paid_at")
    .orderBy(F.col("amount_bucket").desc(), "min_paid_at")
)
