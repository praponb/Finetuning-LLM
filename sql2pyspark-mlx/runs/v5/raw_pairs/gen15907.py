from pyspark.sql import functions as F

result = (
    df_transactions
    .filter((F.col("txn_time") >= "2025-10-06") & (F.col("account_id").isin([10, 14, 15, 30]) | F.col("amount").isNull()))
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy("min_txn_time")
)
