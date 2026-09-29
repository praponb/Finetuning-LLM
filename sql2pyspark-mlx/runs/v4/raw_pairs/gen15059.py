from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "left")
    .groupBy(F.col("a.status"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
    )
    .orderBy("stddev_amount", "status")
    .limit(5)
)
