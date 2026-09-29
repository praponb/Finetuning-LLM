from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.min("amount").alias("min_amount"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("AccountStats"), F.col("accounts.id") == F.col("AccountStats.account_id"), "left")
    .select(
        F.col("accounts.balance"),
        F.col("AccountStats.stddev_amount"),
        F.col("AccountStats.min_amount"),
    )
)
