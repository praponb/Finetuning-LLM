from pyspark.sql import functions as F

account_summary = (
    df_transactions
    .groupBy("account_id")
    .agg(F.min("txn_time").alias("min_txn_time"))
)

result = (
    df_accounts.alias("accounts")
    .join(account_summary.alias("t"), F.col("accounts.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("accounts.customer_id"),
        F.col("accounts.account_type"),
        F.col("t.min_txn_time"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("account_type").desc())
)
