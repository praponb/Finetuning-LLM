from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount") <= 2800)
    .groupBy("account_id")
    .agg(
        F.sum("amount").alias("total_amount"),
        F.max("txn_time").alias("last_txn_time"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .filter(F.col("t.total_amount") >= 12000)
    .select(
        F.col("accounts.opened_date"),
        F.col("t.total_amount"),
        F.col("t.last_txn_time"),
    )
    .orderBy("last_txn_time")
)
