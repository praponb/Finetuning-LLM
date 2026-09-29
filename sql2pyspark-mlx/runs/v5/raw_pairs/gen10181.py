from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("as.max_amount"))
    .orderBy("max_amount")
)
