from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance").between(28000, 29000)).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.txn_time"),
        F.col("t.is_flagged"),
    )
    .orderBy(F.col("txn_time").desc(), "txn_type", "id")
    .limit(1)
)
