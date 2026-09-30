from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount").between(2500, 3900) & ((F.col("txn_time") < "2024-01-08") | F.hour("txn_time").between(8, 16)))
    .groupBy(F.date_format("txn_time", "yyyy-MM").alias("txn_month"))
    .agg(
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
        F.min("txn_time").alias("min_txn_time"),
    )
)
