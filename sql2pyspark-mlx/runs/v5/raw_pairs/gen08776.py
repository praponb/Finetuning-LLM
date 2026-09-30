from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.max("amount").alias("max_amount"))
)

result = (
    df_accounts.alias("a")
    .join(account_summary.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.max_amount") > 3300)
    .select(F.col("a.id"), F.col("a.balance"), F.col("as.max_amount"))
    .orderBy("balance")
)
