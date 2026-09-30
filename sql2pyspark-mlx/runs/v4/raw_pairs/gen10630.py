from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("account_id").isin([19, 25, 33]))
    .groupBy("account_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.balance"), F.col("a.customer_id"), F.col("t.max_amount"))
    .orderBy("max_amount", F.col("customer_id").desc())
)
