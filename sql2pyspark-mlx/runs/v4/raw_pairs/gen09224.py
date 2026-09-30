from pyspark.sql import functions as F

account_totals = (
    df_transactions
    .filter(F.col("account_id").isin([1, 39]))
    .groupBy("account_id")
    .agg(
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_totals.alias("account_totals"), F.col("accounts.id") == F.col("account_totals.account_id"), "inner")
    .filter(F.col("account_totals.credit_count") > 37)
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.balance"),
        F.col("account_totals.credit_count"),
    )
    .orderBy(F.col("credit_count").desc())
)
