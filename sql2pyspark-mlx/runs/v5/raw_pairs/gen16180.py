from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("amount") >= 1640, "high").when(F.col("amount") >= 520, "mid").otherwise("low").alias("amount_bucket"),
        F.quarter("paid_at").alias("paid_quarter"),
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
    )
    .orderBy(F.col("paid_quarter").desc(), "amount_bucket")
)
