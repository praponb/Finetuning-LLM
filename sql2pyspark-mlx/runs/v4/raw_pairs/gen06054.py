from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.status"), F.col("a.balance"), F.col("t.cnt"))
    .orderBy("balance", F.col("id").desc())
)
