from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") >= 38000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_semi")
    .filter(F.hour(F.col("t.txn_time")).between(9, 14))
    .select(
        F.col("t.id"),
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("t.txn_type"),
    )
)
