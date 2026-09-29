from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.status") != "OPEN")
    .groupBy(
        F.when(F.col("a.balance") >= 66000, "high").when(F.col("a.balance") >= 32000, "mid").otherwise("low").alias("balance_bucket"),
    )
    .agg(F.avg(F.col("t.amount")).alias("avg_amount"))
    .filter(F.col("avg_amount") >= 1400)
    .orderBy(F.col("balance_bucket").desc())
    .limit(3)
)
