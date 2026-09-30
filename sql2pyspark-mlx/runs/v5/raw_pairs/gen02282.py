from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "right")
    .groupBy(F.col("a.account_type"))
    .agg(
        F.sum(F.when(F.col("t.txn_type") == "DEBIT", F.col("t.amount")).otherwise(0)).alias("debit_amount"),
        F.sum(F.col("t.amount")).alias("total_amount"),
    )
    .orderBy("account_type")
)
