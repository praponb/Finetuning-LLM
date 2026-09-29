from pyspark.sql import functions as F

transactions_by_account = (
    df_transactions
    .filter(F.col("amount") >= 2700)
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("a")
    .join(transactions_by_account.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(F.col("a.balance"), F.col("t.avg_amount"))
    .orderBy(F.col("balance").desc())
)
