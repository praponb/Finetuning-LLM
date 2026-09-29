from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") >= 3600)
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.sum(F.when(F.col("txn_type") == "REFUND", F.col("amount")).otherwise(0)).alias("refund_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .filter(F.col("tba.refund_amount") > 76)
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("tba.min_txn_time"),
        F.col("tba.refund_amount"),
    )
)
