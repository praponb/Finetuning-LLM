from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("latest_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(F.col("a.balance"), F.col("as.latest_txn_time"))
    .orderBy(F.col("balance").desc())
)
