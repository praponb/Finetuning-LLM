from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("merchant").isin(["Lazada", "Grab", "Shell"]))
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("a")
    .join(account_stats.alias("as"), F.col("a.id") == F.col("as.account_id"), "left")
    .select(
        F.col("a.id"),
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("as.amount_range"),
    )
    .orderBy("status")
)
