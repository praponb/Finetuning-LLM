from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.filter(F.col("balance") >= 46000).alias("a"), F.col("t.account_id") == F.col("a.id"), "left_anti")
    .filter(F.col("t.account_id").isin([19, 40]))
    .select(
        F.col("t.id"),
        F.col("t.amount"),
        F.col("t.account_id"),
        F.col("t.txn_time"),
    )
)
