from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("amount").between(1800, 2100))
    .groupBy("account_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .select(
        F.col("a.id"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("as.avg_amount"),
    )
    .orderBy("avg_amount")
)
