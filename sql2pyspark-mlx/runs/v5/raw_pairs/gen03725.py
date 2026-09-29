from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.count(F.when(F.col("t.txn_type") == "FEE", 1)).alias("fee_count"),
        F.sum(F.when(F.col("t.merchant") == "Shell", 1).otherwise(0)).alias("shell_count"),
    )
)
