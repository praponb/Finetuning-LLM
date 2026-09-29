from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id") == 28)
    .groupBy("account_id")
    .agg(F.sum("amount").alias("total_amount"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .filter(F.col("account_totals.total_amount") >= 7000)
    .select(
        F.col("accounts.id"),
        F.col("accounts.balance"),
        F.col("account_totals.total_amount"),
    )
)
