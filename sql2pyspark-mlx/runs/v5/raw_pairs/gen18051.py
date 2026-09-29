from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.account_type"),
        F.col("account_totals.latest_txn_time"),
    )
    .orderBy("latest_txn_time", "account_type")
)
