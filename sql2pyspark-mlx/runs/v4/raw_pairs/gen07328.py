from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .groupBy(
        F.when(F.col("a.balance") >= 77000, "high").when(F.col("a.balance") >= 8000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.min(F.col("t.txn_time")).alias("min_txn_time"))
    .orderBy("balance_bucket")
)
