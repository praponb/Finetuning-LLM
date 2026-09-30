from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.countDistinct(F.col("t.amount")).alias("unique_accounts"),
        F.max(F.col("t.txn_time")).alias("last_txn_time"),
    )
    .orderBy(F.col("transaction_count").desc())
)
