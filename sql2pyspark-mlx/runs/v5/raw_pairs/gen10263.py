from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .select(F.col("a.customer_id"), F.col("t.avg_amount"))
    .orderBy("customer_id")
)
