from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id").isin([2, 19, 23, 33]))
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.id"), F.col("a.account_type"), F.col("t.amount_count"))
)
