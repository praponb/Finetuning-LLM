from pyspark.sql import functions as F

result = (
    df_transactions.alias("t")
    .join(df_accounts.alias("a"), F.col("t.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.account_type") == "SAVINGS")
    .groupBy(F.col("a.status"))
    .agg(
        F.min(F.col("t.amount")).alias("min_amount"),
        F.sum(F.when(F.col("t.txn_type") == "CREDIT", 1).otherwise(0)).alias("credit_count"),
    )
    .orderBy(F.col("credit_count").desc(), "status")
    .limit(5)
)
