from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "outer")
    .filter(F.col("t.is_flagged"))
    .select(
        F.col("t.account_id"),
        F.col("t.merchant"),
        F.col("t.txn_type"),
        F.col("a.status"),
        F.col("a.customer_id"),
    )
)
