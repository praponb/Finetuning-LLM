from pyspark.sql import functions as F

account_stats = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("txn_type") == "REFUND", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refund"),
        F.sum(F.when(F.col("txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
)

result = (
    df_accounts.alias("accounts")
    .join(account_stats.alias("account_stats"), F.col("accounts.id") == F.col("account_stats.account_id"), "inner")
    .select(
        F.col("accounts.id"),
        F.col("accounts.status"),
        F.col("account_stats.pct_refund"),
        F.col("account_stats.credit_count"),
    )
    .orderBy(F.col("credit_count").desc())
)
