from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .filter(F.hour(F.col("t.txn_time")).between(12, 19))
    .select(
        F.col("a.status"),
        F.col("a.account_type"),
        F.col("t.account_id"),
        F.col("t.is_flagged"),
    )
)
