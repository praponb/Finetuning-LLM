from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id") == 38)
    .groupBy("account_id")
    .agg(F.avg("amount").alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "left")
    .select(F.col("a.id"), F.col("a.balance"), F.col("at.avg_amount"))
)
