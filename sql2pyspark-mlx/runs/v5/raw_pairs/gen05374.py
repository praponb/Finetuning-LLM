from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= "2023-09-15") | F.col("order_id").isin([3, 10, 24, 29]))
)

payments_stats = (
    base
    .groupBy("order_id")
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    payments_stats
    .filter(F.col("min_amount") > 630)
    .select("order_id", "last_paid_at")
)
