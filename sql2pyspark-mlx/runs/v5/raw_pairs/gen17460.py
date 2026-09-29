from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .groupBy(
        F.col("a.account_type"),
        F.when(F.col("a.balance") >= 62000, "high").when(F.col("a.balance") >= 22000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(
        F.sum(F.col("t.amount")).alias("total_amount"),
        F.count(F.col("t.amount")).alias("amount_count"),
    )
)
