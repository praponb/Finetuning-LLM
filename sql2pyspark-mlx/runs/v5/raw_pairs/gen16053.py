from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2025-06-23")
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("first_txn_time"),
        F.sum(F.when(F.col("txn_type") == "DEBIT", F.col("amount")).otherwise(0)).alias("debit_amount"),
    )
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.balance"),
        F.col("a.opened_date"),
        F.col("A.first_txn_time"),
        F.col("A.debit_amount"),
    )
    .orderBy("opened_date")
)
