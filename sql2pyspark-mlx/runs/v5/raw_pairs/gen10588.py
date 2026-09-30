from pyspark.sql import functions as F

base = df_transactions.filter(F.col("merchant") == "Grab")

summary = (
    base
    .groupBy("account_id", F.quarter("txn_time").alias("txn_quarter"))
    .agg(
        F.countDistinct("amount").alias("unique_txn_types"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)

result = (
    summary
    .orderBy("account_id", "txn_quarter")
    .limit(10)
)
