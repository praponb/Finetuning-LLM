from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.min("paid_at").alias("first_paid_at"),
    )
    .filter(F.col("avg_amount") > 870)
    .select("method", "avg_amount", "first_paid_at")
)
