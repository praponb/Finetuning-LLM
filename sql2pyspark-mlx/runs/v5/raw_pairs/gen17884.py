from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]))
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"), "method")
    .agg(
        F.max("paid_at").alias("latest_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 1150)
    .select("paid_month", "method", "latest_paid_at", "avg_amount")
)
