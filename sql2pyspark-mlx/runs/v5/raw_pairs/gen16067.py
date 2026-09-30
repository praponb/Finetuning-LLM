from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_time") < "2023-06-09")
    .groupBy("account_id")
    .agg(F.round(F.stddev("amount"), 2).alias("stddev_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(transactions_by_account.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "left")
    .select(F.col("accounts.id"), F.col("accounts.status"), F.col("t.stddev_amount"))
)
