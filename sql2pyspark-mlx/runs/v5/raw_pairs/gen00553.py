from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .filter(F.col("amount").isNotNull())
    .groupBy("account_id")
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "left")
    .select(
        F.col("accounts.status"),
        F.col("accounts.customer_id"),
        F.col("account_stats.amount_range"),
    )
)
