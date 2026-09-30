from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
        F.max("paid_at").alias("last_paid_at"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("bank_transfer_count") >= 39)
    .orderBy("status")
)
