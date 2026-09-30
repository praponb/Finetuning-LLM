from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "inner")
    .select(
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
        F.col("a.balance"),
        F.col("a.account_type"),
    )
)
