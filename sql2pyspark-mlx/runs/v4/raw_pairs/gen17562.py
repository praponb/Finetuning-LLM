from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id") == 30)
    .groupBy("account_id")
    .agg(
        F.min("txn_time").alias("min_txn_time"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .select(
        F.col("accounts.opened_date"),
        F.col("account_summary.min_txn_time"),
        F.col("account_summary.min_amount"),
    )
)
