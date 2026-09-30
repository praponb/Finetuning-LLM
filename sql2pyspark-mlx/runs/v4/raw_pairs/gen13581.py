from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter((F.col("order_id") == 16) & F.col("method").isin(["cash", "paypal"]))
)

payments_stats = (
    recent_payments
    .groupBy("status")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.max("paid_at").alias("last_paid_at"),
    )
)

result = (
    payments_stats
    .filter(F.col("avg_amount") > 890)
    .select("status", "last_paid_at")
    .orderBy(F.col("last_paid_at").desc(), "status")
)
