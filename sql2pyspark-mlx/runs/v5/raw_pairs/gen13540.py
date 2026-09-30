from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("status") == "FROZEN").alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .select(
        F.col("t.id"),
        F.col("t.is_flagged"),
        F.col("t.amount"),
        F.col("t.txn_type"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
