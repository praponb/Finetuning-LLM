from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id").isin([24, 32]))
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("first_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("t.first_txn_time"),
    )
    .orderBy(F.col("account_type").desc(), "id")
    .limit(50)
)
