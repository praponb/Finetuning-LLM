from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_credit"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.opened_date"),
        F.col("accounts.account_type"),
        F.col("account_totals.pct_credit"),
        F.col("account_totals.amount_range"),
    )
)
