from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("txn_type") == "CREDIT")
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "inner")
    .filter(F.col("as.amount_range") > 25)
    .select(F.col("a.customer_id"), F.col("a.balance"), F.col("as.amount_range"))
)
