from pyspark.sql import functions as F

result = (
    df_transactions.alias("transactions")
    .join(df_accounts.alias("accounts"), F.col("transactions.account_id") == F.col("accounts.id"), "outer")
    .groupBy(
        F.when(F.col("accounts.balance") >= 76000, "high").when(F.col("accounts.balance") >= 10000, "mid").otherwise("low").alias("balance_bucket"),
        F.col("accounts.account_type"),
    )
    .agg(
        F.count(F.col("transactions.amount")).alias("amount_count"),
        F.avg(F.col("transactions.amount")).alias("avg_amount"),
    )
)
