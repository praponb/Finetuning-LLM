from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .filter((F.col("t.merchant") != "Starbucks") & F.col("t.is_flagged"))
    .select(
        F.col("t.id"),
        F.col("t.txn_time"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.col("a.balance"),
        F.col("a.customer_id"),
    )
)
