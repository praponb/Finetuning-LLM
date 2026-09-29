from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("AccountSummary"), F.col("accounts.id") == F.col("AccountSummary.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.status"),
        F.col("AccountSummary.row_count"),
    )
    .orderBy(F.col("opened_date").desc())
)
