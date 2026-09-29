from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("a.id") == F.col("t.account_id"), "left")
    .groupBy(F.col("a.id"), F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.count(F.when(F.col("t.txn_type") == "REFUND", 1)).alias("refund_count"),
        F.round(F.avg(F.col("t.amount")), 2).alias("avg_amount"),
    )
    .filter(F.col("refund_count") >= 5)
    .orderBy("refund_count")
)
