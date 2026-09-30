from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-06-01")
    .groupBy(F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") >= 1600)
)
