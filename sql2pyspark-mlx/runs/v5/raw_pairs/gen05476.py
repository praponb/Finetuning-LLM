from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter((F.col("t.amount") > 1400) & F.col("t.is_flagged"))
    .select(
        F.col("a.account_type"),
        F.col("a.balance"),
        F.col("t.merchant"),
        F.col("t.txn_time"),
    )
)
