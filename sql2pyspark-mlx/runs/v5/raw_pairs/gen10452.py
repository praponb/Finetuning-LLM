from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.customer_id"), F.col("a.balance"), F.col("t.min_txn_time"))
)
