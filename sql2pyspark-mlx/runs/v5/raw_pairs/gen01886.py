from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.hour("txn_time").between(9, 13))
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("A"), F.col("a.id") == F.col("A.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.customer_id"),
        F.col("A.min_txn_time"),
    )
    .orderBy(F.col("customer_id").desc(), "id")
    .limit(5)
)
