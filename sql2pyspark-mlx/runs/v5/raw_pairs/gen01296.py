from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.hour("txn_time").between(9, 15))
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("transactions_by_account"), F.col("accounts.id") == F.col("transactions_by_account.account_id"), "inner")
    .select(F.col("accounts.balance"), F.col("transactions_by_account.latest_txn_time"))
    .orderBy("latest_txn_time")
)
