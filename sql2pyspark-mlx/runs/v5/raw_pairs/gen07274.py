from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("paid_at") >= "2023-06-01")

status_summary = (
    recent_payments
    .groupBy("status", "method")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.count("*").alias("payment_count"),
    )
)

result = (
    status_summary
    .filter(F.col("pct_refunded") >= 31)
    .orderBy("status", "method")
    .limit(5)
)
