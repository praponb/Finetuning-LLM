from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("a.id") == F.col("t.account_id"), "outer")
    .filter(F.col("a.balance") >= 62000)
    .select(
        F.col("t.id"),
        F.col("t.txn_type"),
        F.col("t.is_flagged"),
        F.col("t.merchant"),
        F.col("a.status"),
        F.round(F.col("a.balance"), 1).alias("balance_rounded"),
    )
    .orderBy(F.col("balance_rounded").desc())
)
