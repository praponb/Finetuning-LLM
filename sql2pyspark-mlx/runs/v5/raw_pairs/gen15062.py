from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.balance") >= 26000)
    .select(
        F.col("t.merchant"),
        F.col("t.txn_time"),
        F.col("t.txn_type"),
        F.col("a.balance"),
    )
    .orderBy("txn_type")
)
