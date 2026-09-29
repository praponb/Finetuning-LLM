from pyspark.sql import functions as F

filtered_transactions = (
    df_transactions
    .filter(F.col("merchant").isin(["Amazon", "Shell"]) & (F.col("account_id") == 12))
)

merchant_summary = (
    filtered_transactions
    .groupBy("merchant")
    .agg(
        F.sum(F.when(F.col("txn_type") == "DEBIT", 1).otherwise(0)).alias("debit_count"),
    )
)

result = merchant_summary.orderBy("merchant")
