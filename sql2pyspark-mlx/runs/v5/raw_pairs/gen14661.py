from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED"]) & (F.col("paid_at") >= "2023-11-03"))
    .groupBy("status")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("amount").alias("amount_count"),
        F.avg("amount").alias("avg_amount"),
    )
)
