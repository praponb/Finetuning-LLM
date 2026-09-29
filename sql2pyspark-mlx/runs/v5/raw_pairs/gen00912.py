from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.sum(F.when(F.col("txn_type") == "FEE", 1).otherwise(0)).alias("fee_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("fee_count")
)
