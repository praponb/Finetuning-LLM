from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter(F.col("a.customer_id").isNotNull() & F.col("t.account_id").isin([25, 38]))
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("t.amount"),
        F.col("t.txn_time"),
    )
)
