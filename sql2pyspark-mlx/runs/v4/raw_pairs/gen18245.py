from pyspark.sql import functions as F

result = (
    df_payments
    .agg(
        F.max("paid_at").alias("last_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("avg_amount")
)
