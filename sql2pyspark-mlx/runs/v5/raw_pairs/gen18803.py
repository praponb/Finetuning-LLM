from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id").isin([10, 24, 27, 30]))
    .groupBy("account_id")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    df_accounts.alias("a")
    .join(account_totals.alias("at"), F.col("a.id") == F.col("at.account_id"), "inner")
    .filter(F.col("at.amount_count") >= 8)
    .select(F.col("a.balance"), F.col("at.amount_count"))
    .orderBy(F.col("balance").desc(), F.col("amount_count").desc())
)
