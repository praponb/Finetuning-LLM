from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.col("t.account_id").isin([18, 26, 28]))
    .select(
        F.col("a.account_type"),
        F.col("a.status"),
        F.col("a.customer_id"),
        F.col("t.account_id"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
    )
)
