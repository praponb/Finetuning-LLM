from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_transactions.alias("t"), F.col("t.account_id") == F.col("a.id"), "inner")
    .filter(F.col("t.amount") < 3000)
    .groupBy(F.col("a.id"), F.col("a.account_type"))
    .agg(
        F.count(F.col("t.id")).alias("transaction_count"),
        F.round(F.stddev(F.col("t.amount")), 2).alias("stddev_amount"),
        F.count(F.when(F.col("t.merchant") == "Shell", 1)).alias("shell_count"),
    )
    .orderBy("id")
)
