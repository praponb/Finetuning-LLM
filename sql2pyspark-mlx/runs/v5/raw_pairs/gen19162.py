from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.hour(F.col("t.txn_time")).between(6, 11))
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("a.balance"),
        F.col("a.status"),
        F.col("a.account_type"),
    )
    .orderBy(F.col("balance").desc(), "status")
)
