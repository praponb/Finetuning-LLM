from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("a.customer_id"), "inner")
    .filter(F.col("t.txn_type") == "DEBIT")
    .groupBy(
        F.when(F.col("a.balance") >= 61000, "high").when(F.col("a.balance") >= 20000, "mid").otherwise("low").alias("balance_bucket"),
        F.col("a.status"),
    )
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
)
