from pyspark.sql import functions as F

valid_payments = (
    df_payments
    .filter(F.col("order_id").isin([1, 15, 25]) & F.col("order_id").isNotNull() & F.col("amount").between(840, 1080))
)

summary = (
    valid_payments
    .groupBy("status")
    .agg(F.max("paid_at").alias("latest_paid_at"))
)

result = summary.select("status", "latest_paid_at")
