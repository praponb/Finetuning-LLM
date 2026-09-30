from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("txn_time") >= "2023-07-10")
    .groupBy("account_id")
    .agg(F.max("txn_time").alias("last_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("tba"), F.col("a.id") == F.col("tba.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.opened_date"),
        F.col("tba.last_txn_time"),
    )
    .orderBy(F.col("status").desc(), F.col("last_txn_time").desc())
)
