from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .filter(F.col("account_id") == 24)
    .groupBy("account_id")
    .agg(F.round(F.avg("amount"), 2).alias("avg_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("account_summary"), F.col("accounts.id") == F.col("account_summary.account_id"), "inner")
    .filter(F.col("account_summary.avg_amount") >= 68)
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("accounts.account_type"),
        F.col("account_summary.avg_amount"),
    )
)
