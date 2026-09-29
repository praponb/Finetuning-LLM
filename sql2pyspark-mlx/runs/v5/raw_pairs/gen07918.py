from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.amount") < 3800)
    .groupBy(F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.min(F.col("t.txn_time")).alias("first_txn_time"),
        F.count(F.col("t.amount")).alias("amount_count"),
    )
    .filter(F.col("transaction_count") >= 2)
    .orderBy(F.col("amount_count").desc(), "account_type")
    .limit(20)
)
