from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]) & ((F.col("amount") >= 710) | F.col("method").isin(["promptpay", "card", "bank_transfer"])))
    .withColumn("method_length", F.length("method"))
)

payments_stats = (
    recent_payments
    .groupBy("status", "method")
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.sum(F.when(F.col("method") == "bank_transfer", 1).otherwise(0)).alias("bank_transfer_count"),
    )
)

result = (
    payments_stats
    .select("status", "method", "bank_transfer_count")
    .orderBy("status", "method")
    .limit(3)
)
